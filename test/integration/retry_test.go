//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func cleanupRetryRuns(t *testing.T, jobID string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := database.Exec(`UPDATE job_runs SET status='CANCELLED', finished_at=now()
			WHERE job_id::text=$1 AND status IN ('PENDING','QUEUED')`, jobID); err != nil {
			t.Error(err)
		}
	})
}

func deadRun(t *testing.T) store.Run {
	t.Helper()
	message := queuedDispatch(t, store.Job{})
	worker := registeredWorker(t, 5*time.Minute)
	startAttempt(t, message, worker)
	run, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "permanent failure", Permanent: true})
	if err != nil || run.Status != "DEAD" {
		t.Fatalf("create DEAD source = %+v, %v", run, err)
	}
	cleanupRetryRuns(t, run.JobID)
	return run
}

func TestRetryDeadRunPreservesHistoryAndExecutes(t *testing.T) {
	message := queuedDispatch(t, store.Job{MaxAttempts: 2, BackoffBaseMS: 1, BackoffMaxMS: 1})
	cleanupRetryRuns(t, message.JobID)
	worker := registeredWorker(t, 5*time.Minute)
	for attempt := 1; attempt <= 2; attempt++ {
		message.Attempt = attempt
		startAttempt(t, message, worker)
		if _, err := data.FailRun(context.Background(), message.RunID, attempt, worker, store.Failure{Error: "exhausted"}); err != nil {
			t.Fatal(err)
		}
		if attempt == 1 {
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			err := waitFor(ctx, func() error {
				ok, err := data.QueueRun(ctx, message.RunID)
				if err == nil && !ok {
					return errors.New("retry not due")
				}
				return err
			})
			stop()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	source := waitForRun(t, message.RunID, "DEAD")
	history := func() []byte {
		t.Helper()
		var body []byte
		if err := database.QueryRow(`SELECT jsonb_agg(to_jsonb(a) ORDER BY attempt) FROM run_attempts a WHERE run_id::text=$1`, source.ID).Scan(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	beforeHistory, beforeTime := history(), databaseNow(t)
	beforeRetries := counterValue(t, "retries_total", "")
	var children []store.Run
	for range 2 {
		response := perform(api, http.MethodPost, "/v1/runs/"+source.ID+"/retry", nil, apiKey, "")
		if response.Code != http.StatusCreated {
			t.Fatalf("retry status/body = %d %s", response.Code, response.Body.String())
		}
		var run store.Run
		if err := json.Unmarshal(response.Body.Bytes(), &run); err != nil {
			t.Fatal(err)
		}
		if run.ID == source.ID || run.JobID != source.JobID || run.RetryOf != source.ID || run.Attempt != 1 || run.Status != "PENDING" ||
			run.WorkerID != "" || run.LastError != "" || run.CancelRequested || run.QueuedAt != nil || run.StartedAt != nil || run.FinishedAt != nil ||
			!run.FireTime.Equal(run.ScheduledFor) || run.FireTime.Before(beforeTime) || run.FireTime.After(databaseNow(t)) {
			t.Fatalf("retry inherited source state or wrong identity/time: %+v", run)
		}
		var count int
		if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text=$1`, run.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("new chain already has attempt history: %d, %v", count, err)
		}
		children = append(children, run)
	}
	if children[0].ID == children[1].ID {
		t.Fatal("explicit retries reused a chain")
	}
	assertCounter(t, "retries_total", "", beforeRetries+2)
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumerWithOptions(t, topic, "manual-retry-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, fastWorkerOptions(2))
	startWatcher(t, writer, dlq, fastWatcherOptions())
	for _, child := range children {
		run := waitForRun(t, child.ID, "SUCCEEDED")
		if run.Attempt != 1 || run.RetryOf != source.ID {
			t.Fatalf("retry executed with wrong chain: %+v", run)
		}
		assertAttempt(t, child.ID, 1, "SUCCEEDED", "", nil, 0)
	}
	after, err := data.GetRun(context.Background(), source.ID)
	if err != nil || !reflect.DeepEqual(source, after) || !bytes.Equal(beforeHistory, history()) {
		t.Fatalf("manual retry changed source/history: before %+v, after %+v, error %v", source, after, err)
	}
}

func TestRetryRejectsInvalidSources(t *testing.T) {
	before := counterValue(t, "retries_total", "")
	for _, status := range []string{"PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "CANCELLED", "FAILED"} {
		t.Run(status, func(t *testing.T) {
			message := queuedDispatch(t, store.Job{})
			worker := registeredWorker(t, 5*time.Minute)
			if status == "RUNNING" || status == "SUCCEEDED" {
				startAttempt(t, message, worker)
				if status == "SUCCEEDED" {
					if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker); err != nil || !ok {
						t.Fatalf("complete source = %v, %v", ok, err)
					}
				}
			} else if _, err := database.Exec(`UPDATE job_runs SET status=$2 WHERE id::text=$1`, message.RunID, status); err != nil {
				t.Fatal(err)
			}
			response := perform(api, http.MethodPost, "/v1/runs/"+message.RunID+"/retry", nil, apiKey, "")
			if response.Code != http.StatusConflict {
				t.Fatalf("%s retry status/body = %d %s", status, response.Code, response.Body.String())
			}
			var count int
			if err := database.QueryRow(`SELECT count(*) FROM job_runs WHERE retry_of::text=$1`, message.RunID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("illegal retry created a chain: %d, %v", count, err)
			}
			if status == "RUNNING" {
				if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker); err != nil || !ok {
					t.Fatalf("finish test source = %v, %v", ok, err)
				}
			}
		})
	}
	deleted := deadRun(t)
	if _, err := database.Exec(`UPDATE jobs SET deleted_at=now() WHERE id::text=$1`, deleted.JobID); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id, key string
		status  int
	}{
		{deleted.ID, "", http.StatusUnauthorized},
		{"bad-id", apiKey, http.StatusBadRequest},
		{uuid.NewString(), apiKey, http.StatusNotFound},
		{deleted.ID, apiKey, http.StatusConflict},
	} {
		response := perform(api, http.MethodPost, "/v1/runs/"+tc.id+"/retry", nil, tc.key, "")
		if response.Code != tc.status {
			t.Fatalf("retry rejection = %d %s; want %d", response.Code, response.Body.String(), tc.status)
		}
	}
	assertCounter(t, "retries_total", "", before)
}

func TestConcurrentImmediateRunsCreateSeparateChains(t *testing.T) {
	for _, operation := range []string{"retry", "run-now"} {
		t.Run(operation, func(t *testing.T) {
			source := deadRun(t)
			const callers = 8
			before := counterValue(t, "retries_total", "")
			type result struct {
				run store.Run
				err error
			}
			start, done := make(chan struct{}), make(chan result, callers)
			for range callers {
				go func() {
					<-start
					ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					var run store.Run
					var err error
					if operation == "retry" {
						run, err = data.RetryRun(ctx, source.ID)
					} else {
						run, err = data.RunNow(ctx, source.JobID)
					}
					done <- result{run, err}
				}()
			}
			close(start)
			seen := make(map[string]bool)
			wantLink, wantRetries := "", before
			if operation == "retry" {
				wantLink, wantRetries = source.ID, before+callers
			}
			for range callers {
				result := <-done
				if result.err != nil || result.run.Status != "PENDING" || result.run.RetryOf != wantLink || seen[result.run.ID] {
					// Drain all callers before fixture cleanup, even when one fails.
					t.Errorf("concurrent %s = %+v, error: %v", operation, result.run, result.err)
					continue
				}
				seen[result.run.ID] = true
			}
			assertCounter(t, "retries_total", "", wantRetries)
		})
	}
}

func TestRetryRechecksDeletionAfterJobLockWait(t *testing.T) {
	source := deadRun(t)
	before := counterValue(t, "retries_total", "")
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(`UPDATE jobs SET deleted_at=now() WHERE id::text=$1`, source.JobID); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := data.RetryRun(ctx, source.ID)
		done <- err
	}()
	if err := waitFor(ctx, func() error {
		var blocked bool
		if err := database.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type='Lock' AND query LIKE '%WITH retry_source AS%')`).Scan(&blocked); err != nil {
			return err
		}
		if !blocked {
			return errors.New("retry has not reached the job lock")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := lock.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, store.ErrInvalidTransition) {
		t.Fatalf("retry ignored committed deletion: %v", err)
	}
	var count int
	if err := database.QueryRow(`SELECT count(*) FROM job_runs WHERE retry_of::text=$1`, source.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("deleted job gained a retry: %d, %v", count, err)
	}
	assertCounter(t, "retries_total", "", before)
}

func TestRetryPausedCronAndRetryChildLink(t *testing.T) {
	job := dueCronJob(t, "fire_once", databaseNow(t).Add(time.Hour))
	if _, err := data.SetJobPaused(context.Background(), job.ID, true); err != nil {
		t.Fatal(err)
	}
	source, err := data.RunNow(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	worker := registeredWorker(t, 5*time.Minute)
	for range 2 {
		if ok, err := data.QueueRun(context.Background(), source.ID); err != nil || !ok {
			t.Fatalf("queue source = %v, %v", ok, err)
		}
		if ok, err := data.StartRun(context.Background(), source.ID, 1, worker); err != nil || !ok {
			t.Fatalf("start source = %v, %v", ok, err)
		}
		if _, err := data.FailRun(context.Background(), source.ID, 1, worker, store.Failure{Error: "permanent", Permanent: true}); err != nil {
			t.Fatal(err)
		}
		retry, err := data.RetryRun(context.Background(), source.ID)
		if err != nil || retry.RetryOf != source.ID || retry.JobID != job.ID || retry.Attempt != 1 {
			t.Fatalf("retry chain link = %+v, %v", retry, err)
		}
		source = retry
	}
	updated, err := data.GetJob(context.Background(), job.ID)
	if err != nil || !updated.Paused || !updated.NextFireAt.Equal(*job.NextFireAt) {
		t.Fatalf("manual retry changed paused cron: %+v, %v", updated, err)
	}
}
