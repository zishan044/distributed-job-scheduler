//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/queue"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func queuedDispatch(t *testing.T, job store.Job) store.DispatchMessage {
	t.Helper()
	job.Name = "reliability-" + uuid.NewString()
	if job.Type == "" {
		job.Type = "handler"
	}
	if job.Payload == nil {
		job.Payload = json.RawMessage(`{"handler":"noop"}`)
	}
	created, err := data.CreateJob(context.Background(), job, timePtr(time.Now().Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	runs, err := data.ListJobRuns(context.Background(), created.ID, 1, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("list runs = %+v, %v", runs, err)
	}
	message := store.DispatchMessage{
		RunID: runs[0].ID, JobID: created.ID, Attempt: runs[0].Attempt,
		Type: created.Type, Payload: created.Payload, TimeoutMS: created.TimeoutMS, FireTime: runs[0].FireTime,
	}
	t.Cleanup(func() { _, _ = data.CancelRun(context.Background(), message.RunID) })
	if ok, err := data.QueueRun(context.Background(), message.RunID); err != nil || !ok {
		t.Fatalf("queue run = %v, %v", ok, err)
	}
	return message
}

func startAttempt(t *testing.T, message store.DispatchMessage, worker string) {
	t.Helper()
	if ok, err := data.StartRun(context.Background(), message.RunID, message.Attempt, worker); err != nil || !ok {
		t.Fatalf("start run = %v, %v", ok, err)
	}
}

func waitForRun(t *testing.T, id, status string) store.Run {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	var run store.Run
	err := waitFor(ctx, func() error {
		var err error
		run, err = data.GetRun(ctx, id)
		if err != nil {
			return err
		}
		if run.Status != status {
			return fmt.Errorf("run status %s; want %s", run.Status, status)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("wait for %s: last run %+v: %v", status, run, err)
	}
	return run
}

func TestFailureRetriesAndFencesAttempts(t *testing.T) {
	failed := counterValue(t, "executions_total", "failed")
	succeeded := counterValue(t, "executions_total", "succeeded")
	retries := counterValue(t, "retries_total", "")
	stale := counterValue(t, "stale_completions_total", "")
	message := queuedDispatch(t, store.Job{MaxAttempts: 3, BackoffBaseMS: 10, BackoffMaxMS: 20})
	worker := registeredWorker(t, 5*time.Minute)
	startAttempt(t, message, worker)
	for _, tuple := range []struct {
		attempt int
		worker  string
	}{{2, worker}, {1, uuid.NewString()}} {
		if run, err := data.FailRun(context.Background(), message.RunID, tuple.attempt, tuple.worker, store.Failure{Error: "stale"}); err != nil || run.ID != "" {
			t.Fatalf("stale failure = %+v, %v", run, err)
		}
	}
	run, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "network error"})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "PENDING" || run.Attempt != 2 || !run.FireTime.Equal(message.FireTime) || run.LastError != "network error" ||
		run.WorkerID != "" || run.QueuedAt != nil || run.StartedAt != nil || run.FinishedAt != nil {
		t.Fatalf("retry state = %+v", run)
	}
	assertAttempt(t, message.RunID, 1, "FAILED", "network error", &run.ScheduledFor, 10*time.Millisecond)
	if stale, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "duplicate"}); err != nil || stale.ID != "" {
		t.Fatalf("duplicate failure = %+v, %v", stale, err)
	}
	if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker); err != nil || ok {
		t.Fatalf("stale success = %v, %v", ok, err)
	}
	queueRetry(t, message.RunID)
	message.Attempt = 2
	nextWorker := registeredWorker(t, 5*time.Minute)
	latencySum := metric(t, "start_latency_seconds", "").GetHistogram().GetSampleSum()
	startAttempt(t, message, nextWorker)
	startedRun := waitForRun(t, message.RunID, "RUNNING")
	gotLatency := metric(t, "start_latency_seconds", "").GetHistogram().GetSampleSum() - latencySum
	wantLatency := startedRun.StartedAt.Sub(startedRun.ScheduledFor).Seconds()
	if math.Abs(gotLatency-wantLatency) > 0.000001 {
		t.Fatalf("retry start latency = %v, want current scheduled_for delta %v", gotLatency, wantLatency)
	}
	if stale, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "late failure"}); err != nil || stale.ID != "" {
		t.Fatalf("late failure = %+v, %v", stale, err)
	}
	if ok, err := data.SucceedRun(context.Background(), message.RunID, 2, nextWorker); err != nil || !ok {
		t.Fatalf("retry success = %v, %v", ok, err)
	}
	assertAttempt(t, message.RunID, 2, "SUCCEEDED", "", nil, 0)
	assertCounter(t, "executions_total", "failed", failed+1)
	assertCounter(t, "executions_total", "succeeded", succeeded+1)
	assertCounter(t, "retries_total", "", retries+1)
	assertCounter(t, "stale_completions_total", "", stale+5)
}

func TestStartClaimWaitsForDispatchTransaction(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%v", rollback), func(t *testing.T) {
			starts := metric(t, "start_latency_seconds", "").GetHistogram().GetSampleCount()
			claimedBefore := counterValue(t, "watcher_claimed_total", "")
			dispatchedBefore := metric(t, "dispatch_latency_seconds", "").GetHistogram().GetSampleCount()
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			job, err := data.CreateJob(ctx, store.Job{
				Name: "publish-race-" + uuid.NewString(), Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
			}, timePtr(time.Now().Add(-time.Second)))
			if err != nil {
				t.Fatal(err)
			}
			published, release := make(chan store.DispatchMessage, 1), make(chan struct{})
			releaseDispatch := sync.OnceFunc(func() { close(release) })
			defer releaseDispatch()
			dispatched := make(chan error, 1)
			publishErr := errors.New("publish failed")
			var dispatchCount int
			go func() {
				count, err := data.DispatchDue(ctx, 100, func(batch []store.DispatchMessage) error {
					for _, message := range batch {
						if message.JobID == job.ID {
							published <- message
						}
					}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
					if rollback {
						return publishErr
					}
					return nil
				})
				dispatchCount = count
				dispatched <- err
			}()
			var message store.DispatchMessage
			select {
			case message = <-published:
			case <-ctx.Done():
				t.Fatal("dispatcher did not publish")
			}
			t.Cleanup(func() { _, _ = data.CancelRun(context.Background(), message.RunID) })
			worker := registeredWorker(t, 5*time.Minute)
			claimed := make(chan bool, 1)
			claimErr := make(chan error, 1)
			go func() {
				ok, err := data.StartRun(ctx, message.RunID, message.Attempt, worker)
				claimed <- ok
				claimErr <- err
			}()
			select {
			case <-claimed:
				t.Fatal("start gate returned before dispatch committed or rolled back")
			case <-time.After(50 * time.Millisecond):
			}
			releaseDispatch()
			if err := <-dispatched; (rollback && !errors.Is(err, publishErr)) || (!rollback && err != nil) {
				t.Fatalf("dispatch result = %v", err)
			}
			if ok, err := <-claimed, <-claimErr; err != nil || ok == rollback {
				t.Fatalf("start after dispatch = %v, %v; rollback=%v", ok, err, rollback)
			}
			assertCounter(t, "watcher_claimed_total", "", claimedBefore+float64(dispatchCount))
			if got := metric(t, "dispatch_latency_seconds", "").GetHistogram().GetSampleCount(); got != dispatchedBefore+uint64(dispatchCount) {
				t.Fatalf("dispatch samples = %d, want %d", got, dispatchedBefore+uint64(dispatchCount))
			}
			wantStarts := starts
			if !rollback {
				wantStarts++
				run := waitForRun(t, message.RunID, "RUNNING")
				if run.StartedAt.Before(*run.QueuedAt) {
					t.Fatal("start timestamp excluded the wait for dispatch to commit")
				}
			}
			if got := metric(t, "start_latency_seconds", "").GetHistogram().GetSampleCount(); got != wantStarts {
				t.Fatalf("accepted start samples = %d, want %d", got, wantStarts)
			}
			if !rollback {
				if ok, err := data.SucceedRun(ctx, message.RunID, 1, worker); err != nil || !ok {
					t.Fatalf("finish dispatched run = %v, %v", ok, err)
				}
			}
		})
	}
}

func TestFailureBackoffBoundsAndOverflow(t *testing.T) {
	message := queuedDispatch(t, store.Job{MaxAttempts: 6, BackoffBaseMS: 10, BackoffMaxMS: 25})
	worker := registeredWorker(t, 5*time.Minute)
	for attempt, bound := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond} {
		message.Attempt = attempt + 1
		startAttempt(t, message, worker)
		run, err := data.FailRun(context.Background(), message.RunID, message.Attempt, worker, store.Failure{Error: "retry"})
		if err != nil || run.Status != "PENDING" {
			t.Fatalf("failure = %+v, %v", run, err)
		}
		assertAttempt(t, message.RunID, message.Attempt, "FAILED", "retry", &run.ScheduledFor, bound)
		if attempt < 3 {
			queueRetry(t, message.RunID)
		}
	}

	message = queuedDispatch(t, store.Job{MaxAttempts: 1<<31 - 1, BackoffBaseMS: 1<<31 - 1, BackoffMaxMS: 1<<31 - 1})
	message.Attempt = 1<<31 - 2
	if _, err := database.Exec(`UPDATE job_runs SET attempt=$2 WHERE id::text=$1`, message.RunID, message.Attempt); err != nil {
		t.Fatal(err)
	}
	startAttempt(t, message, worker)
	run, err := data.FailRun(context.Background(), message.RunID, message.Attempt, worker, store.Failure{Error: "large attempt"})
	if err != nil || run.Attempt != 1<<31-1 || run.Status != "PENDING" {
		t.Fatalf("large-attempt failure = %+v, %v", run, err)
	}
	assertAttempt(t, message.RunID, message.Attempt, "FAILED", "large attempt", &run.ScheduledFor, (1<<31-1)*time.Millisecond)
}

func TestPermanentExhaustedAndCancelledFinalization(t *testing.T) {
	for _, tc := range []struct {
		name      string
		permanent bool
		attempts  int
		cancel    bool
		succeed   bool
		status    string
	}{
		{"permanent", true, 5, false, false, "DEAD"},
		{"exhausted", false, 1, false, false, "DEAD"},
		{"cancel before failure", false, 5, true, false, "CANCELLED"},
		{"cancel before permanent failure", true, 5, true, false, "CANCELLED"},
		{"cancel before success", false, 5, true, true, "CANCELLED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcomes := counterValue(t, "executions_total", strings.ToLower(tc.status))
			successes := counterValue(t, "executions_total", "succeeded")
			message := queuedDispatch(t, store.Job{MaxAttempts: tc.attempts})
			worker := registeredWorker(t, 5*time.Minute)
			startAttempt(t, message, worker)
			if tc.cancel {
				if _, err := data.CancelRun(context.Background(), message.RunID); err != nil {
					t.Fatal(err)
				}
			}
			errorText := "failure"
			if tc.succeed {
				errorText = ""
				if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker); err != nil || !ok {
					t.Fatalf("finalize success = %v, %v", ok, err)
				}
			} else if _, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: errorText, Permanent: tc.permanent}); err != nil {
				t.Fatal(err)
			}
			run := waitForRun(t, message.RunID, tc.status)
			if run.Attempt != 1 || run.FinishedAt == nil {
				t.Fatalf("terminal run = %+v", run)
			}
			assertAttempt(t, message.RunID, 1, tc.status, errorText, nil, 0)
			if stale, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "duplicate"}); err != nil || stale.ID != "" {
				t.Fatalf("terminal failure changed run: %+v, %v", stale, err)
			}
			assertCounter(t, "executions_total", strings.ToLower(tc.status), outcomes+1)
			assertCounter(t, "executions_total", "succeeded", successes)
		})
	}
}

func queueRetry(t *testing.T, id string) {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		ok, err := data.QueueRun(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("retry not due yet")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizationRollsBackWithoutAttemptHistory(t *testing.T) {
	for _, fail := range []bool{true, false} {
		t.Run(fmt.Sprintf("fail=%v", fail), func(t *testing.T) {
			failed := counterValue(t, "executions_total", "failed")
			succeeded := counterValue(t, "executions_total", "succeeded")
			retries := counterValue(t, "retries_total", "")
			stale := counterValue(t, "stale_completions_total", "")
			message := queuedDispatch(t, store.Job{})
			worker := registeredWorker(t, 5*time.Minute)
			startAttempt(t, message, worker)
			if _, err := database.Exec(`DELETE FROM run_attempts WHERE run_id::text=$1`, message.RunID); err != nil {
				t.Fatal(err)
			}
			var err error
			if fail {
				_, err = data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "failure"})
			} else {
				_, err = data.SucceedRun(context.Background(), message.RunID, 1, worker)
			}
			if err == nil {
				t.Fatal("finalization without attempt history succeeded")
			}
			run := waitForRun(t, message.RunID, "RUNNING")
			if run.Attempt != 1 || run.WorkerID != worker || run.FinishedAt != nil || run.LastError != "" {
				t.Fatalf("failed transaction changed run: %+v", run)
			}
			assertCounter(t, "executions_total", "failed", failed)
			assertCounter(t, "executions_total", "succeeded", succeeded)
			assertCounter(t, "retries_total", "", retries)
			assertCounter(t, "stale_completions_total", "", stale)
			if _, err := database.Exec(`INSERT INTO run_attempts (run_id, attempt, worker_id, started_at)
				SELECT id, attempt, worker_id, started_at FROM job_runs WHERE id::text=$1`, message.RunID); err != nil {
				t.Fatal(err)
			}
			if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker); err != nil || !ok {
				t.Fatalf("finalization after restoring history = %v, %v", ok, err)
			}
		})
	}
}

func assertAttempt(t *testing.T, id string, attempt int, outcome, failure string, scheduled *time.Time, maxDelay time.Duration) {
	t.Helper()
	var gotOutcome, gotFailure string
	var finished time.Time
	if err := database.QueryRow(`SELECT outcome, COALESCE(error, ''), finished_at FROM run_attempts WHERE run_id::text=$1 AND attempt=$2`, id, attempt).Scan(&gotOutcome, &gotFailure, &finished); err != nil {
		t.Fatal(err)
	}
	if gotOutcome != outcome || gotFailure != failure {
		t.Fatalf("attempt history = %q, %q; want %q, %q", gotOutcome, gotFailure, outcome, failure)
	}
	if scheduled != nil {
		if delay := scheduled.Sub(finished); delay < 0 || delay > maxDelay {
			t.Fatalf("DB-clock retry delay = %s; want 0-%s", delay, maxDelay)
		}
	}
}

func startConsumer(t *testing.T, topic, group string, client *http.Client, dlq *kafka.Writer, concurrency int) <-chan error {
	t.Helper()
	return startConsumerWithOptions(t, topic, group, client, dlq, executorlib.DefaultOptions(concurrency))
}

func startConsumerWithOptions(t *testing.T, topic, group string, client *http.Client, dlq *kafka.Writer, options executorlib.Options) <-chan error {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	reader := kafkaReader(topic, group)
	done := make(chan error, 1)
	go func() {
		err := executorlib.Consume(ctx, reader, data, client, dlq, logger, options)
		_ = reader.Close()
		done <- err
		close(done)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("consumer stopped: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop")
		}
		_ = reader.Close()
	})
	return done
}

func publishDispatch(t *testing.T, writer *kafka.Writer, message store.DispatchMessage) {
	t.Helper()
	value, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(message.RunID), Value: value}); err != nil {
		t.Fatal(err)
	}
}

func readDeadLetter(t *testing.T, topic string) queue.DeadLetter {
	t.Helper()
	reader := kafkaReader(topic, "dlq-reader-"+uuid.NewString())
	defer reader.Close()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	message, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var letter queue.DeadLetter
	if err := json.Unmarshal(message.Value, &letter); err != nil {
		t.Fatal(err)
	}
	return letter
}

func TestConsumerRetriesHTTPThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	payload, _ := json.Marshal(map[string]string{"url": server.URL})
	message := queuedDispatch(t, store.Job{Type: "http", Payload: payload, MaxAttempts: 3, BackoffBaseMS: 10, BackoffMaxMS: 20})
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumer(t, topic, "retry-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, 2)
	publishDispatch(t, writer, message)
	run := waitForRun(t, message.RunID, "PENDING")
	if run.Attempt != 2 {
		t.Fatalf("retry attempt = %d", run.Attempt)
	}
	queueRetry(t, message.RunID)
	message.Attempt = 2
	publishDispatch(t, writer, message)
	waitForRun(t, message.RunID, "SUCCEEDED")
	if calls.Load() != 2 {
		t.Fatalf("HTTP executions = %d; want 2", calls.Load())
	}
	assertAttempt(t, message.RunID, 2, "SUCCEEDED", "", nil, 0)
	assertEmptyTopic(t, dlqTopic)
}

func TestConsumerRoutesPermanentAndExhaustedFailuresToDLQ(t *testing.T) {
	for _, permanent := range []bool{true, false} {
		t.Run(fmt.Sprintf("permanent=%v", permanent), func(t *testing.T) {
			letters := counterValue(t, "dlq_total", "")
			dead := counterValue(t, "executions_total", "dead")
			status, attempts := http.StatusServiceUnavailable, 1
			if permanent {
				status, attempts = http.StatusBadRequest, 5
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			payload, _ := json.Marshal(map[string]string{"url": server.URL})
			message := queuedDispatch(t, store.Job{Type: "http", Payload: payload, MaxAttempts: attempts})
			topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
			writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
			t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
			startConsumer(t, topic, "dead-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, 2)
			publishDispatch(t, writer, message)
			waitForRun(t, message.RunID, "DEAD")
			letter := readDeadLetter(t, dlqTopic)
			if letter.Message == nil || letter.Message.RunID != message.RunID || letter.Message.Attempt != 1 || !bytes.Equal(letter.Message.Payload, payload) || letter.Source != nil || letter.Error == "" {
				t.Fatalf("DEAD letter = %+v", letter)
			}
			assertCounter(t, "dlq_total", "", letters+1)
			assertCounter(t, "executions_total", "dead", dead+1)
		})
	}
}

func TestConsumerCancelledResultDoesNotRetryOrPublish(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			releaseJob := sync.OnceFunc(func() { close(release) })
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-release:
					w.WriteHeader(status)
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			defer releaseJob()
			payload, _ := json.Marshal(map[string]string{"url": server.URL})
			message := queuedDispatch(t, store.Job{Type: "http", Payload: payload})
			topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
			writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
			t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
			startConsumer(t, topic, "cancel-result-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, 1)
			publishDispatch(t, writer, message)
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("HTTP job did not start")
			}
			if _, err := data.CancelRun(context.Background(), message.RunID); err != nil {
				t.Fatal(err)
			}
			releaseJob()
			run := waitForRun(t, message.RunID, "CANCELLED")
			if run.Attempt != 1 {
				t.Fatalf("cancelled run spent a retry: %+v", run)
			}
			assertEmptyTopic(t, dlqTopic)
		})
	}
}

func TestConsumerMalformedMessageThenValidWork(t *testing.T) {
	letters := counterValue(t, "dlq_total", "")
	durations := metric(t, "execution_duration_seconds", "handler").GetHistogram().GetSampleCount()
	message := queuedDispatch(t, store.Job{})
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumer(t, topic, "malformed-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, 2)
	raw := []byte{'{', 0xff}
	if err := writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(message.RunID), Value: raw}); err != nil {
		t.Fatal(err)
	}
	publishDispatch(t, writer, message)
	waitForRun(t, message.RunID, "SUCCEEDED")
	letter := readDeadLetter(t, dlqTopic)
	if letter.Message != nil || letter.Error == "" || letter.Source == nil || !bytes.Equal(letter.Source.RawValue, raw) ||
		letter.Source.Topic != topic || !bytes.Equal(letter.Source.Key, []byte(message.RunID)) || letter.Source.Offset != 0 {
		t.Fatalf("malformed letter = %+v", letter)
	}
	assertCounter(t, "dlq_total", "", letters+1)
	if got := metric(t, "execution_duration_seconds", "handler").GetHistogram().GetSampleCount(); got != durations+1 {
		t.Fatalf("execution samples = %d, want %d; malformed messages must not execute", got, durations+1)
	}
}

func unavailableDLQ() *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP("127.0.0.1:1"), Topic: "jobs.dlq", RequiredAcks: kafka.RequireAll,
		BatchTimeout: time.Millisecond, WriteTimeout: 100 * time.Millisecond, MaxAttempts: 1,
	}
}

func TestConsumerMalformedDLQFailureDoesNotCommit(t *testing.T) {
	letters := counterValue(t, "dlq_total", "")
	message := queuedDispatch(t, store.Job{})
	topic, group := createKafkaTopic(t), "uncommitted-"+uuid.NewString()
	writer, dlq := kafkaWriter(topic), unavailableDLQ()
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	bad := kafka.Message{Key: []byte(message.RunID), Value: []byte("{")}
	if err := writer.WriteMessages(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	publishDispatch(t, writer, message)
	done := startConsumer(t, topic, group, executorlib.HTTPClient(nil), dlq, 2)
	select {
	case err := <-done:
		if err == nil || errors.Is(err, context.Canceled) {
			t.Fatalf("consumer error = %v; want DLQ error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop on failed malformed publication")
	}
	waitForRun(t, message.RunID, "QUEUED")
	reader := kafkaReader(topic, group)
	defer reader.Close()
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	redelivery, err := reader.FetchMessage(ctx)
	if err != nil || !bytes.Equal(redelivery.Value, bad.Value) || redelivery.Offset != 0 {
		t.Fatalf("uncommitted redelivery = %+v, %v", redelivery, err)
	}
	assertCounter(t, "dlq_total", "", letters)
}

func TestConsumerDeadRowSurvivesDLQFailure(t *testing.T) {
	letters := counterValue(t, "dlq_total", "")
	dead := counterValue(t, "executions_total", "dead")
	message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"missing"}`)})
	topic := createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), unavailableDLQ()
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumer(t, topic, "best-effort-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, 1)
	publishDispatch(t, writer, message)
	run := waitForRun(t, message.RunID, "DEAD")
	if run.LastError == "" {
		t.Fatal("DEAD row lost its failure reason")
	}
	assertAttempt(t, message.RunID, 1, "DEAD", run.LastError, nil, 0)
	// The same consumer must keep accepting work after best-effort publication fails.
	next := queuedDispatch(t, store.Job{})
	publishDispatch(t, writer, next)
	waitForRun(t, next.RunID, "SUCCEEDED")
	assertCounter(t, "dlq_total", "", letters)
	assertCounter(t, "executions_total", "dead", dead+1)
}

func assertEmptyTopic(t *testing.T, topic string) {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for partition := range 3 {
		conn, err := kafka.DialLeader(ctx, "tcp", kafkaBrokers[0], topic, partition)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		last, err := conn.ReadLastOffset()
		_ = conn.Close()
		if err != nil || last != 0 {
			t.Fatalf("DLQ partition %d last offset = %d, %v; want empty", partition, last, err)
		}
	}
}
