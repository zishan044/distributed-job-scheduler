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
	job, initial := h.scheduleHTTP(ctx)
	run := h.waitRun(ctx, api.StatusQueued)
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
	run = h.waitRun(ctx, api.StatusSucceeded)
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
	if run.Attempt != 1 || run.ID != initial.ID || !run.FireTime.Equal(initial.FireTime) {
		t.Fatalf("run identity changed: %+v", run)
	}
	h.event("verified", map[string]any{"run": run, "attempts": attempts, "http": calls, "kafka_lag": 0})
}
