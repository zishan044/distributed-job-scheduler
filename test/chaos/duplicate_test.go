//go:build chaos

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/queue"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

func TestDuplicateDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(suiteCtx, 2*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	for _, item := range h.executors {
		h.pause(ctx, item)
	}
	now := time.Now().UTC()
	job, err := h.client.ScheduleHTTP(ctx, api.CreateJobRequest{
		Name: "chaos-duplicate" + h.path, ScheduledFor: &now,
		MaxAttempts: 3, BackoffBaseMS: 500, BackoffMaxMS: 500, TimeoutMS: 120000,
	}, api.HTTPPayload{URL: "http://chaos-runner:8090" + h.path, Method: "POST"})
	if err != nil {
		t.Fatal(err)
	}
	runs, err := h.client.ListRuns(ctx, job.ID, 100, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("initial runs = %v, %v", runs, err)
	}
	h.runID = runs[0].ID
	var run api.Run
	if err := waitFor(ctx, func(ctx context.Context) error {
		run, err = h.client.Get(ctx, h.runID)
		if err != nil || run.Status != api.StatusQueued {
			return fmt.Errorf("waiting for QUEUED: %+v, %v", run, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls := target.calls(h.path); len(calls) != 0 {
		t.Fatalf("execution occurred while all executors were paused: %+v", calls)
	}
	message := store.DispatchMessage{RunID: run.ID, JobID: job.ID, Attempt: run.Attempt,
		Type: job.Type, Payload: job.Payload, TimeoutMS: job.TimeoutMS, FireTime: run.FireTime}
	value, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	writer := &kafka.Writer{Addr: kafka.TCP("kafka:9092"), Topic: "jobs.run",
		Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, WriteTimeout: 5 * time.Second,
		MaxAttempts: 1, BatchTimeout: 5 * time.Millisecond}
	publishCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	err = writer.WriteMessages(publishCtx,
		kafka.Message{Key: []byte(run.ID), Value: value}, kafka.Message{Key: []byte(run.ID), Value: value})
	stop()
	closeErr := writer.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("duplicate publication = %v; close = %v", err, closeErr)
	}
	h.event("published_two_duplicates", message)
	for _, item := range h.executors {
		h.resume(ctx, item)
	}
	if err := waitFor(ctx, func(ctx context.Context) error {
		run, err = h.client.Get(ctx, h.runID)
		if err != nil || run.Status != api.StatusSucceeded {
			return fmt.Errorf("waiting for SUCCEEDED: %+v, %v", run, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	broker := &kafka.Client{Addr: kafka.TCP("kafka:9092"), Transport: transport}
	if err := waitFor(ctx, func(ctx context.Context) error {
		lag, err := queue.ConsumerLag(ctx, broker, "jobs.run", "executors")
		if err != nil || lag != 0 {
			return fmt.Errorf("remaining Kafka lag = %d: %v", lag, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Check after all three messages were committed, not just after first success.
	attempts, err := h.data.ChaosAttempts(ctx, h.runID)
	if err != nil || len(attempts) != 1 || attempts[0].Attempt != 1 || attempts[0].Outcome != api.StatusSucceeded {
		t.Fatalf("attempt history = %+v, %v", attempts, err)
	}
	calls := target.calls(h.path)
	if len(calls) != 1 || calls[0].IdempotencyKey != h.runID || calls[0].Attempt != 1 ||
		calls[0].FinishedAt == nil || calls[0].Cancelled {
		t.Fatalf("HTTP executions = %+v; want exactly one completed call", calls)
	}
	if run.Attempt != 1 || run.ID != runs[0].ID || !run.FireTime.Equal(runs[0].FireTime) {
		t.Fatalf("run identity changed: %+v", run)
	}
	h.event("verified", map[string]any{"run": run, "attempts": attempts, "http": calls, "kafka_lag": 0})
}
