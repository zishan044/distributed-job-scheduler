//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func TestRetryAfterFailureScheduling(t *testing.T) {
	worker := registeredWorker(t, time.Minute)
	var future time.Time
	if err := database.QueryRow(`SELECT date_trunc('second', clock_timestamp()+interval '1 hour')`).Scan(&future); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		failure  store.Failure
		attempts int
		cancel   bool
		status   string
		minDelay time.Duration
	}{
		{"seconds above cap", store.Failure{Error: "429", RetryAfter: time.Hour}, 3, false, "PENDING", time.Hour},
		{"HTTP date above cap", store.Failure{Error: "429", RetryAt: future}, 3, false, "PENDING", 0},
		{"no hint", store.Failure{Error: "429"}, 3, false, "PENDING", 0},
		{"past date", store.Failure{Error: "429", RetryAt: future.Add(-2 * time.Hour)}, 3, false, "PENDING", 0},
		{"exhaustion ignores hint", store.Failure{Error: "429", RetryAfter: time.Hour}, 1, false, "DEAD", 0},
		{"cancellation ignores hint", store.Failure{Error: "429", RetryAfter: time.Hour}, 1, true, "CANCELLED", 0},
		{"permanent failure ignores hint", store.Failure{Error: "invalid", Permanent: true, RetryAfter: time.Hour}, 3, false, "DEAD", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := queuedDispatch(t, store.Job{MaxAttempts: tc.attempts, BackoffBaseMS: 10, BackoffMaxMS: 20})
			startAttempt(t, message, worker)
			if tc.cancel {
				if _, err := data.CancelRun(context.Background(), message.RunID); err != nil {
					t.Fatal(err)
				}
			}
			run, err := data.FailRun(context.Background(), message.RunID, 1, worker, tc.failure)
			if err != nil || run.Status != tc.status || !run.FireTime.Equal(message.FireTime) {
				t.Fatalf("Retry-After failure = %+v, %v", run, err)
			}
			if tc.status != "PENDING" {
				if run.Attempt != 1 || !run.ScheduledFor.Equal(message.FireTime) {
					t.Fatalf("terminal hint changed scheduling: %+v", run)
				}
				return
			}
			if run.Attempt != 2 || run.WorkerID != "" {
				t.Fatalf("Retry-After did not reset execution ownership: %+v", run)
			}
			var finished time.Time
			if err := database.QueryRow(`SELECT finished_at FROM run_attempts WHERE run_id::text=$1 AND attempt=1`, run.ID).Scan(&finished); err != nil {
				t.Fatal(err)
			}
			if tc.minDelay > 0 {
				if delay := run.ScheduledFor.Sub(finished); delay < tc.minDelay || delay > tc.minDelay+time.Second {
					t.Fatalf("Retry-After minimum = %s; want at least %s", delay, tc.minDelay)
				}
			} else if tc.failure.RetryAt.Equal(future) {
				if !run.ScheduledFor.Equal(future) {
					t.Fatalf("HTTP date schedule = %s; want %s", run.ScheduledFor, future)
				}
			} else {
				assertAttempt(t, run.ID, 1, "FAILED", tc.failure.Error, &run.ScheduledFor, 20*time.Millisecond)
			}
		})
	}
	for _, hint := range []string{"relative after lock wait", "date expires during lock wait"} {
		t.Run(hint, func(t *testing.T) {
			message := queuedDispatch(t, store.Job{BackoffBaseMS: 10, BackoffMaxMS: 20})
			startAttempt(t, message, worker)
			lock, err := database.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Rollback() }()
			if _, err := lock.Exec(`SELECT id FROM job_runs WHERE id::text=$1 FOR UPDATE`, message.RunID); err != nil {
				t.Fatal(err)
			}
			failure := store.Failure{Error: "429", RetryAfter: time.Second}
			if hint == "date expires during lock wait" {
				failure.RetryAfter = 0
				if err := database.QueryRow(`SELECT clock_timestamp()+interval '100 milliseconds'`).Scan(&failure.RetryAt); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				_, err := data.FailRun(ctx, message.RunID, 1, worker, failure)
				done <- err
			}()
			time.Sleep(300 * time.Millisecond)
			if err := lock.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if failure.RetryAfter > 0 {
				var remaining float64
				if err := database.QueryRow(`SELECT extract(epoch FROM scheduled_for-clock_timestamp()) FROM job_runs WHERE id::text=$1`, message.RunID).Scan(&remaining); err != nil || remaining < 0.9 || remaining > 1.0 {
					t.Fatalf("relative minimum after lock wait = %fs, %v", remaining, err)
				}
			} else {
				run := waitForRun(t, message.RunID, "PENDING")
				assertAttempt(t, run.ID, 1, "FAILED", "429", &run.ScheduledFor, 20*time.Millisecond)
			}
		})
	}
}

func TestHTTPRetryAfterAndIdempotencyThroughRuntimes(t *testing.T) {
	for _, format := range []string{"seconds", "date"} {
		t.Run(format, func(t *testing.T) {
			type observation struct {
				at, minimum        time.Time
				key, attempt, user string
				body               string
			}
			seen := make(chan observation, 4)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				item := observation{at: time.Now(), key: r.Header.Get("Idempotency-Key"), attempt: r.Header.Get("X-Job-Attempt"), user: r.Header.Get("X-User"), body: string(body)}
				if calls.Add(1) == 1 {
					item.minimum = item.at.Add(2 * time.Second)
					value := "2"
					if format == "date" {
						item.minimum = item.at.UTC().Add(3 * time.Second).Truncate(time.Second)
						value = item.minimum.Format(http.TimeFormat)
					}
					seen <- item
					w.Header().Set("Retry-After", value)
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				seen <- item
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(server.Close)
			payload, _ := json.Marshal(map[string]any{"url": server.URL, "method": "POST", "body": "work", "headers": map[string]string{
				"idempotency-key": "payload-key", "IDEMPOTENCY-KEY": "second-payload-key", "x-job-attempt": "999", "X-User": "preserved",
			}})
			message := queuedDispatch(t, store.Job{Type: "http", Payload: payload, MaxAttempts: 3, BackoffBaseMS: 10, BackoffMaxMS: 20})
			topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
			writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
			t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
			startConsumerWithOptions(t, topic, "http-reliability-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, fastWorkerOptions(1))
			startWatcher(t, writer, dlq, fastWatcherOptions())
			publishDispatch(t, writer, message)
			retry := waitForRun(t, message.RunID, "PENDING")
			first := <-seen
			if retry.Attempt != 2 || retry.ScheduledFor.Before(first.minimum) || !retry.FireTime.Equal(message.FireTime) {
				t.Fatalf("429 retry schedule = %+v, minimum=%s", retry, first.minimum)
			}
			completed := waitForRun(t, message.RunID, "SUCCEEDED")
			second := <-seen
			if completed.Attempt != 2 || calls.Load() != 2 || second.at.Before(retry.ScheduledFor) {
				t.Fatalf("retry executed early or twice: %+v, at=%s, calls=%d", completed, second.at, calls.Load())
			}
			if first.key != message.RunID || second.key != first.key || first.attempt != "1" || second.attempt != "2" || first.user != "preserved" || second.user != first.user || first.body != "work" || second.body != first.body {
				t.Fatalf("HTTP identity/payload changed across retries: %+v, %+v", first, second)
			}
			assertAttempt(t, message.RunID, 1, "FAILED", "HTTP target returned status 429", nil, 0)
			assertAttempt(t, message.RunID, 2, "SUCCEEDED", "", nil, 0)
			assertEmptyTopic(t, dlqTopic)
		})
	}
}

func TestM3RecoveryThenCooperativeCancellation(t *testing.T) {
	message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"sleep","duration_ms":60000}`), TimeoutMS: 120000, BackoffBaseMS: 10, BackoffMaxMS: 20})
	crashed := registeredWorker(t, time.Minute)
	startAttempt(t, message, crashed)
	expireWorker(t, crashed)
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumerWithOptions(t, topic, "recovery-cancel-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, fastWorkerOptions(1))
	startWatcher(t, writer, dlq, fastWatcherOptions())
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		run, err := data.GetRun(ctx, message.RunID)
		if err != nil {
			return err
		}
		if run.Status != "RUNNING" || run.Attempt != 2 || run.WorkerID == crashed {
			return errors.New("recovered attempt has not started")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	response := perform(api, http.MethodPost, "/v1/runs/"+message.RunID+"/cancel", nil, apiKey, "")
	if response.Code != http.StatusOK {
		t.Fatalf("cancel recovered attempt = %d, %s", response.Code, response.Body.String())
	}
	run := waitForRun(t, message.RunID, "CANCELLED")
	if run.Attempt != 2 || !run.FireTime.Equal(message.FireTime) {
		t.Fatalf("recovered cancellation changed fire/attempt: %+v", run)
	}
	assertAttempt(t, message.RunID, 1, "FAILED", "worker lease expired", nil, 0)
	assertAttempt(t, message.RunID, 2, "CANCELLED", "job cancellation requested", nil, 0)
	assertEmptyTopic(t, dlqTopic)
}
