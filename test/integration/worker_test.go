//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/worker"
)

func TestPublicWorkerRuntime(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	client := &kafka.Client{Addr: kafka.TCP(kafkaBrokers...)}
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{Topic: "jobs.run", NumPartitions: 3, ReplicationFactor: 1},
			{Topic: "jobs.dlq", NumPartitions: 3, ReplicationFactor: 1},
		},
	})
	stop()
	if err != nil {
		t.Fatal(err)
	}
	for topic, err := range response.Errors {
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			t.Fatalf("create %s: %v", topic, err)
		}
	}
	writer := kafkaWriter("jobs.run")
	defer func() { _ = writer.Close() }()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	application := "worker-" + uuid.NewString()
	config := worker.Config{
		DatabaseURL:  databaseURL + "&application_name=" + application,
		KafkaBrokers: kafkaBrokers, Concurrency: 4, HTTPAddr: addr, Logger: logger,
	}
	w, err := worker.New(config)
	if err != nil {
		t.Fatal(err)
	}
	jobs := make(chan worker.Job, 1)
	if err := w.Register("custom", func(_ context.Context, job worker.Job) error { jobs <- job; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := w.Register("panic-retry", func(_ context.Context, job worker.Job) error {
		if job.Attempt == 1 {
			panic("temporary panic")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Register("permanent", func(context.Context, worker.Job) error {
		return fmt.Errorf("wrapped: %w", worker.Permanent(errors.New("invalid input")))
	}); err != nil {
		t.Fatal(err)
	}
	cancellations := make(chan error, 2)
	if err := w.Register("blocking", func(ctx context.Context, _ worker.Job) error {
		<-ctx.Done()
		cancellations <- ctx.Err()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker stopped: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	ctx, stop = context.WithTimeout(context.Background(), 10*time.Second)
	err = waitFor(ctx, func() error {
		response, err := httpClient.Get("http://" + addr + "/healthz")
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("health status %d", response.StatusCode)
		}
		return nil
	})
	stop()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("accepted concurrent Run")
	}
	if err := w.Register("late", func(context.Context, worker.Job) error { return nil }); err == nil {
		t.Fatal("accepted late registration")
	}

	t.Run("identity and complete payload", func(t *testing.T) {
		message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"custom","nested":{"value":42},"error":false,"duration_ms":"custom"}`)})
		publishDispatch(t, writer, message)
		waitForRun(t, message.RunID, "SUCCEEDED")
		select {
		case got := <-jobs:
			var compact bytes.Buffer
			if err := json.Compact(&compact, message.Payload); err != nil {
				t.Fatal(err)
			}
			if got.RunID != message.RunID || got.JobID != message.JobID || got.Attempt != message.Attempt ||
				!got.FireTime.Equal(message.FireTime) || got.TimeoutMS != message.TimeoutMS || !bytes.Equal(got.Payload, compact.Bytes()) {
				t.Fatalf("callback job = %+v; message %+v", got, message)
			}
		case <-time.After(time.Second):
			t.Fatal("callback not invoked")
		}
	})
	t.Run("panic retries through the normal path", func(t *testing.T) {
		message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"panic-retry"}`), MaxAttempts: 2, BackoffBaseMS: 10, BackoffMaxMS: 10})
		publishDispatch(t, writer, message)
		run := waitForRun(t, message.RunID, "PENDING")
		if run.Attempt != 2 || !strings.Contains(run.LastError, "temporary panic") {
			t.Fatalf("panic retry = %+v", run)
		}
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := waitFor(ctx, func() error {
			ok, err := data.QueueRun(ctx, run.ID)
			if err == nil && !ok {
				err = errors.New("retry not due")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		message.Attempt = run.Attempt
		publishDispatch(t, writer, message)
		waitForRun(t, message.RunID, "SUCCEEDED")
	})
	t.Run("wrapped permanent error", func(t *testing.T) {
		message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"permanent"}`), MaxAttempts: 5})
		publishDispatch(t, writer, message)
		run := waitForRun(t, message.RunID, "DEAD")
		if run.Attempt != 1 || !strings.Contains(run.LastError, "invalid input") {
			t.Fatalf("permanent result = %+v", run)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"blocking"}`), TimeoutMS: 25, MaxAttempts: 1})
		publishDispatch(t, writer, message)
		waitForRun(t, message.RunID, "DEAD")
		if err := <-cancellations; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("callback timeout = %v", err)
		}
	})
	t.Run("running cancellation via heartbeat", func(t *testing.T) {
		message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"blocking"}`), TimeoutMS: 30000})
		publishDispatch(t, writer, message)
		waitForRun(t, message.RunID, "RUNNING")
		if _, err := data.CancelRun(context.Background(), message.RunID); err != nil {
			t.Fatal(err)
		}
		waitForRun(t, message.RunID, "CANCELLED")
		if err := <-cancellations; !errors.Is(err, context.Canceled) {
			t.Fatalf("callback cancellation = %v", err)
		}
	})
	t.Run("builtins retained", func(t *testing.T) {
		for _, payload := range []string{`{"handler":"noop"}`, `{"handler":"sleep","duration_ms":1}`, `{"handler":"fail","permanent":true}`} {
			message := queuedDispatch(t, store.Job{Payload: json.RawMessage(payload)})
			publishDispatch(t, writer, message)
			status := "SUCCEEDED"
			if strings.Contains(payload, "fail") {
				status = "DEAD"
			}
			waitForRun(t, message.RunID, status)
		}
	})
	responseHTTP, err := httpClient.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(responseHTTP.Body)
	_ = responseHTTP.Body.Close()
	if err != nil || responseHTTP.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("inflight_jobs 0")) ||
		!bytes.Contains(body, []byte(`execution_duration_seconds_count{type="handler"}`)) {
		t.Fatalf("worker metrics = %d, %s, %v", responseHTTP.StatusCode, body, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not join")
	}
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("worker HTTP listener leaked: %v", err)
	}
	defer func() { _ = listener.Close() }()
	assertWorkerPoolClosed(t, application)

	t.Run("HTTP bind failure closes dependencies", func(t *testing.T) {
		application := "worker-" + uuid.NewString()
		config.DatabaseURL = databaseURL + "&application_name=" + application
		failed, err := worker.New(config)
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := failed.Run(ctx); err == nil || !strings.Contains(err.Error(), "address already in use") {
			t.Fatalf("bind failure = %v", err)
		}
		assertWorkerPoolClosed(t, application)
	})
	t.Run("HTTP disabled", func(t *testing.T) {
		config.HTTPAddr = ""
		config.DatabaseURL = databaseURL
		disabled, err := worker.New(config)
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- disabled.Run(ctx) }()
		defer func() {
			stop()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("disabled HTTP shutdown: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("disabled HTTP worker did not stop")
			}
		}()
		message := queuedDispatch(t, store.Job{})
		publishDispatch(t, writer, message)
		waitForRun(t, message.RunID, "SUCCEEDED")
	})
}

func assertWorkerPoolClosed(t *testing.T, application string) {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		var count int
		if err := database.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", application).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("%d worker connections remain", count)
		}
		return nil
	}); err != nil {
		t.Fatalf("worker pool leaked: %v", err)
	}
}
