//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func TestDeleteJobAPIAndHistory(t *testing.T) {
	job := dueCronJob(t, "fire_once", databaseNow(t).Add(time.Hour))
	worker := registeredWorker(t, 5*time.Minute)
	runs := make(map[string]store.Run)
	for _, status := range []string{"PENDING", "QUEUED", "RUNNING", "SUCCEEDED", "DEAD", "CANCELLED"} {
		run, err := data.RunNow(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if status == "CANCELLED" {
			if _, err := data.CancelRun(context.Background(), run.ID); err != nil {
				t.Fatal(err)
			}
		} else if status != "PENDING" {
			if ok, err := data.QueueRun(context.Background(), run.ID); err != nil || !ok {
				t.Fatalf("queue fixture = %v, %v", ok, err)
			}
			if status != "QUEUED" {
				if ok, err := data.StartRun(context.Background(), run.ID, 1, worker); err != nil || !ok {
					t.Fatalf("start fixture = %v, %v", ok, err)
				}
				if status == "SUCCEEDED" {
					if ok, err := data.SucceedRun(context.Background(), run.ID, 1, worker); err != nil || !ok {
						t.Fatalf("complete fixture = %v, %v", ok, err)
					}
				} else if status == "DEAD" {
					if _, err := data.FailRun(context.Background(), run.ID, 1, worker, store.Failure{Error: "permanent", Permanent: true}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		runs[status] = waitForRun(t, run.ID, status)
	}
	for _, tc := range []struct {
		id, key string
		status  int
	}{{job.ID, "", 401}, {"bad-id", apiKey, 400}, {uuid.NewString(), apiKey, 404}} {
		response := perform(api, http.MethodDelete, "/v1/jobs/"+tc.id, nil, tc.key, "")
		if response.Code != tc.status {
			t.Fatalf("delete rejection = %d %s; want %d", response.Code, response.Body.String(), tc.status)
		}
	}
	var firstDeletion *time.Time
	for range 2 {
		response := perform(api, http.MethodDelete, "/v1/jobs/"+job.ID, nil, apiKey, "")
		var deleted store.Job
		if response.Code != http.StatusOK {
			t.Fatalf("DELETE status/body = %d %s", response.Code, response.Body.String())
		}
		if err := json.Unmarshal(response.Body.Bytes(), &deleted); err != nil {
			t.Fatal(err)
		}
		if deleted.DeletedAt == nil || !deleted.NextFireAt.Equal(*job.NextFireAt) || deleted.ID != job.ID {
			t.Fatalf("delete changed definition or omitted tombstone: %+v", deleted)
		}
		if firstDeletion != nil && !deleted.DeletedAt.Equal(*firstDeletion) {
			t.Fatal("repeated deletion changed its timestamp")
		}
		firstDeletion = deleted.DeletedAt
	}
	for status, before := range runs {
		after, err := data.GetRun(context.Background(), before.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch status {
		case "PENDING", "QUEUED":
			if after.Status != "CANCELLED" || after.FinishedAt == nil || !after.FireTime.Equal(before.FireTime) {
				t.Fatalf("waiting run not cancelled: %+v", after)
			}
		case "RUNNING":
			if after.Status != "RUNNING" || !after.CancelRequested || after.FinishedAt != nil {
				t.Fatalf("running cancellation was not cooperative: %+v", after)
			}
		default:
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("delete changed terminal history: before %+v, after %+v", before, after)
			}
		}
		if response := perform(api, http.MethodGet, "/v1/runs/"+before.ID, nil, apiKey, ""); response.Code != http.StatusOK {
			t.Fatalf("deleted run history unreadable: %d", response.Code)
		}
	}
	for _, suffix := range []string{"", "/runs"} {
		if response := perform(api, http.MethodGet, "/v1/jobs/"+job.ID+suffix, nil, apiKey, ""); response.Code != http.StatusOK {
			t.Fatalf("deleted job/history unreadable: %d", response.Code)
		}
	}
	for _, suffix := range []string{"run-now", "pause", "resume"} {
		if response := perform(api, http.MethodPost, "/v1/jobs/"+job.ID+"/"+suffix, nil, apiKey, ""); response.Code != http.StatusConflict {
			t.Fatalf("deleted job accepted %s: %d", suffix, response.Code)
		}
	}
	if response := perform(api, http.MethodPost, "/v1/runs/"+runs["DEAD"].ID+"/retry", nil, apiKey, ""); response.Code != http.StatusConflict {
		t.Fatalf("deleted job accepted retry: %d", response.Code)
	}
	heartbeat, err := data.HeartbeatWorker(context.Background(), worker, 5*time.Minute)
	if err != nil || len(heartbeat.Cancellations) != 1 || heartbeat.Cancellations[0].RunID != runs["RUNNING"].ID {
		t.Fatalf("delete did not deliver heartbeat cancellation: %+v, %v", heartbeat, err)
	}
	finished, err := data.FailRun(context.Background(), runs["RUNNING"].ID, 1, worker, store.Failure{Error: "deleted"})
	if err != nil || finished.Status != "CANCELLED" || finished.Attempt != 1 {
		t.Fatalf("deleted running job retried: %+v, %v", finished, err)
	}
	assertAttempt(t, finished.ID, 1, "CANCELLED", "deleted", nil, 0)
}

func TestDeleteJobCooperativeCancellationRuntime(t *testing.T) {
	message := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"sleep","duration_ms":60000}`), MaxAttempts: 3})
	pending, err := data.RunNow(context.Background(), message.JobID)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := data.RunNow(context.Background(), message.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := data.QueueRun(context.Background(), queued.ID); err != nil || !ok {
		t.Fatalf("queue fixture = %v, %v", ok, err)
	}
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumerWithOptions(t, topic, "delete-runtime-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, fastWorkerOptions(1))
	publishDispatch(t, writer, message)
	waitForRun(t, message.RunID, "RUNNING")
	beforeRetries := counterValue(t, "retries_total", "")
	if _, err := data.DeleteJob(context.Background(), message.JobID); err != nil {
		t.Fatal(err)
	}
	stale := message
	stale.RunID, stale.FireTime = queued.ID, queued.FireTime
	publishDispatch(t, writer, stale)
	for _, id := range []string{message.RunID, pending.ID, queued.ID} {
		waitForRun(t, id, "CANCELLED")
	}
	assertAttempt(t, message.RunID, 1, "CANCELLED", "job cancellation requested", nil, 0)
	var count int
	if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text IN ($1,$2)`, pending.ID, queued.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("stale delivery started a deleted job: %d, %v", count, err)
	}
	assertCounter(t, "retries_total", "", beforeRetries)
	assertEmptyTopic(t, dlqTopic)
}

func TestDeleteJobCancellationFailureRollsBack(t *testing.T) {
	message := queuedDispatch(t, store.Job{})
	constraint := fmt.Sprintf(`ALTER TABLE job_runs ADD CONSTRAINT delete_test_cancel_failure
		CHECK (job_id <> '%s'::uuid OR status <> 'CANCELLED') NOT VALID`, message.JobID)
	if _, err := database.Exec(constraint); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := database.Exec(`ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS delete_test_cancel_failure`); err != nil {
			t.Error(err)
		}
	})
	if _, err := data.DeleteJob(context.Background(), message.JobID); err == nil {
		t.Fatal("forced cancellation failure committed deletion")
	}
	job, err := data.GetJob(context.Background(), message.JobID)
	if err != nil || job.DeletedAt != nil {
		t.Fatalf("failed deletion left tombstone: %+v, %v", job, err)
	}
	waitForRun(t, message.RunID, "QUEUED")
	if _, err := database.Exec(`ALTER TABLE job_runs DROP CONSTRAINT delete_test_cancel_failure`); err != nil {
		t.Fatal(err)
	}
	if _, err := data.DeleteJob(context.Background(), message.JobID); err != nil {
		t.Fatal(err)
	}
	waitForRun(t, message.RunID, "CANCELLED")
}

func TestDeleteJobBlocksConcurrentCreators(t *testing.T) {
	job := dueCronJob(t, "fire_once", databaseNow(t).Add(-time.Hour))
	source, err := data.RunNow(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	worker := registeredWorker(t, 5*time.Minute)
	if ok, err := data.QueueRun(context.Background(), source.ID); err != nil || !ok {
		t.Fatalf("queue source = %v, %v", ok, err)
	}
	if ok, err := data.StartRun(context.Background(), source.ID, 1, worker); err != nil || !ok {
		t.Fatalf("start source = %v, %v", ok, err)
	}
	if _, err := data.FailRun(context.Background(), source.ID, 1, worker, store.Failure{Error: "permanent", Permanent: true}); err != nil {
		t.Fatal(err)
	}
	pending, err := data.RunNow(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(`SELECT id FROM job_runs WHERE id::text=$1 FOR UPDATE`, pending.ID); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	deleted := make(chan error, 1)
	go func() { _, err := data.DeleteJob(ctx, job.ID); deleted <- err }()
	waitBlocked := func(pattern string, want int) {
		t.Helper()
		if err := waitFor(ctx, func() error {
			var count int
			if err := database.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity
				WHERE wait_event_type='Lock' AND query LIKE $1`, pattern).Scan(&count); err != nil {
				return err
			}
			if count < want {
				return errors.New("operation has not reached its lock")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitBlocked("%SELECT id FROM job_runs WHERE job_id=$1%", 1)
	created := make(chan error, 2)
	go func() { _, err := data.RunNow(ctx, job.ID); created <- err }()
	go func() { _, err := data.RetryRun(ctx, source.ID); created <- err }()
	waitBlocked("%WITH %", 2)
	if count, err := data.MaterializeCron(ctx, 100); err != nil || count != 0 {
		t.Fatalf("materializer ignored deleting job lock: %d, %v", count, err)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-created; !errors.Is(err, store.ErrInvalidTransition) {
			t.Fatalf("creator ignored committed deletion: %v", err)
		}
	}
	runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
	if err != nil || len(runs) != 2 {
		t.Fatalf("deletion admitted new runs: %+v, %v", runs, err)
	}
	waitForRun(t, pending.ID, "CANCELLED")
}

func TestDeleteJobExecutionRaces(t *testing.T) {
	for _, operation := range []string{"start", "succeed", "fail", "sweep", "materialize"} {
		t.Run(operation, func(t *testing.T) {
			for range 6 {
				ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
				var jobID string
				var message store.DispatchMessage
				worker := registeredWorker(t, 5*time.Minute)
				if operation == "materialize" {
					job := dueCronJob(t, "fire_once", databaseNow(t).Add(-time.Hour))
					jobID = job.ID
				} else {
					message = queuedDispatch(t, store.Job{MaxAttempts: 3, BackoffBaseMS: 1, BackoffMaxMS: 1})
					jobID = message.JobID
					if operation != "start" {
						startAttempt(t, message, worker)
					}
					if operation == "sweep" {
						expireWorker(t, worker)
					}
				}
				start, deleted, done := make(chan struct{}), make(chan error, 1), make(chan error, 1)
				go func() { <-start; _, err := data.DeleteJob(ctx, jobID); deleted <- err }()
				go func() {
					<-start
					var err error
					switch operation {
					case "start":
						_, err = data.StartRun(ctx, message.RunID, 1, worker)
					case "succeed":
						_, err = data.SucceedRun(ctx, message.RunID, 1, worker)
					case "fail":
						_, err = data.FailRun(ctx, message.RunID, 1, worker, store.Failure{Error: "retryable"})
					case "sweep":
						_, err = data.Sweep(ctx, 100, time.Minute)
					case "materialize":
						_, err = data.MaterializeCron(ctx, 100)
					}
					done <- err
				}()
				close(start)
				deleteErr, opErr := <-deleted, <-done
				stop()
				if deleteErr != nil || opErr != nil {
					t.Fatalf("delete/%s race failed: %v, %v", operation, deleteErr, opErr)
				}
				if operation == "sweep" {
					// A sweep may skip a run locked by deletion; the next pass cancels it.
					if _, err := data.Sweep(context.Background(), 100, time.Minute); err != nil {
						t.Fatal(err)
					}
				}
				runs, err := data.ListJobRuns(context.Background(), jobID, 10, 0)
				if err != nil {
					t.Fatal(err)
				}
				for _, run := range runs {
					if run.Status == "RUNNING" && operation == "start" && run.CancelRequested {
						if ok, err := data.SucceedRun(context.Background(), run.ID, 1, worker); err != nil || !ok {
							t.Fatalf("finalize deleted start = %v, %v", ok, err)
						}
					} else if run.Status != "CANCELLED" && !(operation == "succeed" && run.Status == "SUCCEEDED") {
						t.Fatalf("delete/%s left executable work: %+v", operation, run)
					}
				}
				if _, err := data.RunNow(context.Background(), jobID); !errors.Is(err, store.ErrInvalidTransition) {
					t.Fatalf("deleted job accepted new run: %v", err)
				}
			}
		})
	}
}
