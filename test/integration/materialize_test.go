//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func cleanupCronJob(t *testing.T, id string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := database.Exec(`UPDATE jobs SET paused=true WHERE id::text=$1`, id); err != nil {
			t.Error(err)
		}
		if _, err := database.Exec(`UPDATE job_runs SET status='CANCELLED', finished_at=now()
			WHERE job_id::text=$1 AND status IN ('PENDING','QUEUED')`, id); err != nil {
			t.Error(err)
		}
	})
}

func dueCronJob(t *testing.T, policy string, first time.Time) store.Job {
	t.Helper()
	job, err := data.CreateJob(context.Background(), store.Job{
		Name: "materialize-" + uuid.NewString(), Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
		CronExpr: "* * * * *", MisfirePolicy: policy, NextFireAt: &first,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cleanupCronJob(t, job.ID)
	return job
}

func databaseNow(t *testing.T) time.Time {
	t.Helper()
	var now time.Time
	if err := database.QueryRow(`SELECT now()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func TestCronMaterializeDowntime(t *testing.T) {
	for _, policy := range []string{"fire_once", "skip", "fire_all"} {
		t.Run(policy, func(t *testing.T) {
			now := databaseNow(t)
			first := now.Truncate(time.Minute).Add(-3 * time.Minute)
			job := dueCronJob(t, policy, first)
			count, err := data.MaterializeCron(context.Background(), 1000)
			if err != nil {
				t.Fatal(err)
			}
			runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
			if err != nil || count != len(runs) {
				t.Fatalf("materialized %d runs, listed %d: %v", count, len(runs), err)
			}
			if (policy == "fire_once" && count != 1) || (policy == "skip" && count != 0) ||
				(policy == "fire_all" && (count < 4 || count > 5)) {
				t.Fatalf("%s materialized %d fires after downtime", policy, count)
			}
			for i, run := range runs {
				want := first
				if policy == "fire_all" {
					want = first.Add(time.Duration(len(runs)-1-i) * time.Minute)
				}
				if run.Status != "PENDING" || run.Attempt != 1 || !run.FireTime.Equal(want) || !run.ScheduledFor.Equal(want) {
					t.Fatalf("incorrect materialized run: %+v; want fire %s", run, want)
				}
			}
			updated, err := data.GetJob(context.Background(), job.ID)
			if err != nil || updated.NextFireAt == nil || !updated.NextFireAt.After(now) || updated.NextFireAt.After(databaseNow(t).Add(time.Minute)) {
				t.Fatalf("cron cursor = %v, error = %v; want next future tick", updated.NextFireAt, err)
			}
		})
	}
}

func TestCronMaterializeCatchUpBoundsAndContinuation(t *testing.T) {
	now := databaseNow(t)
	job := dueCronJob(t, "fire_all", now.Truncate(time.Minute).Add(-48*time.Hour))
	for _, limit := range []int{1000, 7} {
		before, err := data.GetJob(context.Background(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		count, err := data.MaterializeCron(context.Background(), limit)
		if err != nil || count != min(limit, 100) {
			t.Fatalf("bounded catch-up = %d, %v; want %d", count, err, min(limit, 100))
		}
		after, err := data.GetJob(context.Background(), job.ID)
		if err != nil || after.NextFireAt == nil || after.NextFireAt.After(now) {
			t.Fatalf("remaining backlog cursor = %v, %v", after.NextFireAt, err)
		}
		if limit == 7 && !after.NextFireAt.Equal(before.NextFireAt.Add(7*time.Minute)) {
			t.Fatalf("cursor skipped remaining backlog: before %s, after %s", before.NextFireAt, after.NextFireAt)
		}
	}
	var count int
	var first, last time.Time
	if err := database.QueryRow(`SELECT count(*), min(fire_time), max(fire_time) FROM job_runs WHERE job_id::text=$1`, job.ID).Scan(&count, &first, &last); err != nil {
		t.Fatal(err)
	}
	if count != 107 || first.Before(now.Add(-24*time.Hour)) || last.Sub(first) != 106*time.Minute {
		t.Fatalf("catch-up history = %d fires, %s to %s; want 107 contiguous fires within window", count, first, last)
	}
}

func TestConcurrentCronMaterializersSkipLocksAndDeduplicate(t *testing.T) {
	first := databaseNow(t).Truncate(time.Minute).Add(-3 * time.Minute)
	held := dueCronJob(t, "fire_once", first)
	job := dueCronJob(t, "fire_all", first)
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(`SELECT id FROM jobs WHERE id::text=$1 FOR UPDATE`, held.ID); err != nil {
		t.Fatal(err)
	}
	// Simulate a run already present while the cursor still points at that fire.
	if _, err := database.Exec(`INSERT INTO job_runs (id,job_id,fire_time,scheduled_for,status) VALUES ($1,$2,$3,$3,'PENDING')`, uuid.NewString(), job.ID, first); err != nil {
		t.Fatal(err)
	}
	type result struct {
		count int
		err   error
	}
	done, start := make(chan result, 2), make(chan struct{})
	for range 2 {
		go func() {
			<-start
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			count, err := data.MaterializeCron(ctx, 100)
			done <- result{count, err}
		}()
	}
	close(start)
	created := 0
	for range 2 {
		result := <-done
		if result.err != nil {
			t.Fatal(result.err)
		}
		created += result.count
	}
	var total, distinct int
	if err := database.QueryRow(`SELECT count(*),count(DISTINCT fire_time) FROM job_runs WHERE job_id::text=$1`, job.ID).Scan(&total, &distinct); err != nil {
		t.Fatal(err)
	}
	if total != distinct || total != created+1 || total < 4 || total > 5 {
		t.Fatalf("concurrent materialization = %d new, %d total, %d distinct", created, total, distinct)
	}
	unchanged, err := data.GetJob(context.Background(), held.ID)
	if err != nil || !unchanged.NextFireAt.Equal(first) {
		t.Fatalf("locked job cursor changed: %+v, %v", unchanged, err)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if count, err := data.MaterializeCron(context.Background(), 100); err != nil || count < 1 {
		t.Fatalf("released job was not materialized: %d, %v", count, err)
	}
}

func TestCronMaterializeGlobalRunBudget(t *testing.T) {
	first := databaseNow(t).Truncate(time.Minute).Add(-10 * time.Minute)
	a, b := dueCronJob(t, "fire_all", first), dueCronJob(t, "fire_all", first.Add(time.Minute))
	if count, err := data.MaterializeCron(context.Background(), 15); err != nil || count != 15 {
		t.Fatalf("global run budget = %d, %v; want 15", count, err)
	}
	var count int
	if err := database.QueryRow(`SELECT count(*) FROM job_runs WHERE job_id::text IN ($1,$2)`, a.ID, b.ID).Scan(&count); err != nil || count != 15 {
		t.Fatalf("pass exceeded its run budget: %d, %v", count, err)
	}
}

func TestCronMaterializeCursorFailureRollsBackRuns(t *testing.T) {
	first := databaseNow(t).Add(-time.Hour).Truncate(time.Microsecond)
	job := dueCronJob(t, "fire_once", first)
	constraint := fmt.Sprintf(`ALTER TABLE jobs ADD CONSTRAINT cron_test_cursor_failure
		CHECK (id <> '%s'::uuid OR next_fire_at <= TIMESTAMPTZ '%s') NOT VALID`, job.ID, first.Format(time.RFC3339Nano))
	if _, err := database.Exec(constraint); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := database.Exec(`ALTER TABLE jobs DROP CONSTRAINT IF EXISTS cron_test_cursor_failure`); err != nil {
			t.Error(err)
		}
	})
	if count, err := data.MaterializeCron(context.Background(), 100); err == nil || count != 0 {
		t.Fatalf("forced cursor failure = %d, %v; want rollback", count, err)
	}
	runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
	if err != nil || len(runs) != 0 {
		t.Fatalf("failed transaction left runs: %+v, %v", runs, err)
	}
	updated, err := data.GetJob(context.Background(), job.ID)
	if err != nil || !updated.NextFireAt.Equal(first) {
		t.Fatalf("failed transaction advanced cursor: %+v, %v", updated, err)
	}
	if _, err := database.Exec(`ALTER TABLE jobs DROP CONSTRAINT cron_test_cursor_failure`); err != nil {
		t.Fatal(err)
	}
	if count, err := data.MaterializeCron(context.Background(), 100); err != nil || count != 1 {
		t.Fatalf("recovery after rollback = %d, %v", count, err)
	}
}

func TestCronMaterializeExclusionsAndLegacyCursor(t *testing.T) {
	first := databaseNow(t).Add(-time.Hour)
	paused, deleted := dueCronJob(t, "fire_once", first), dueCronJob(t, "fire_once", first)
	future := dueCronJob(t, "fire_once", first.Add(2*time.Hour))
	legacy := dueCronJob(t, "fire_once", first)
	if _, err := database.Exec(`UPDATE jobs SET paused=true WHERE id::text=$1`, paused.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`UPDATE jobs SET deleted_at=now() WHERE id::text=$1`, deleted.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`UPDATE jobs SET next_fire_at=NULL WHERE id::text=$1`, legacy.ID); err != nil {
		t.Fatal(err)
	}
	if count, err := data.MaterializeCron(context.Background(), 100); err != nil || count != 0 {
		t.Fatalf("excluded/legacy materialization = %d, %v", count, err)
	}
	for _, job := range []store.Job{paused, deleted, future, legacy} {
		runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
		if err != nil || len(runs) != 0 {
			t.Fatalf("excluded/legacy job gained runs: %+v, %v", runs, err)
		}
		updated, err := data.GetJob(context.Background(), job.ID)
		if err != nil || updated.NextFireAt == nil {
			t.Fatalf("missing cron cursor: %+v, %v", updated, err)
		}
		if job.ID == legacy.ID {
			if !updated.NextFireAt.After(first.Add(time.Hour)) {
				t.Fatalf("legacy cursor was not initialized to a future tick: %+v", updated)
			}
		} else if !updated.NextFireAt.Equal(*job.NextFireAt) {
			t.Fatalf("excluded job cursor changed: %+v", updated)
		}
	}
}

func TestTwoWatchersMaterializeAndExecuteCron(t *testing.T) {
	first := databaseNow(t).Add(-time.Minute).Truncate(time.Microsecond)
	job := dueCronJob(t, "fire_once", first)
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumerWithOptions(t, topic, "cron-e2e-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, fastWorkerOptions(2))
	for range 2 {
		startWatcher(t, writer, dlq, fastWatcherOptions())
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	var id string
	if err := waitFor(ctx, func() error {
		return database.QueryRowContext(ctx, `SELECT id::text FROM job_runs WHERE job_id::text=$1 AND fire_time=$2`, job.ID, first).Scan(&id)
	}); err != nil {
		t.Fatal(err)
	}
	run := waitForRun(t, id, "SUCCEEDED")
	if run.Attempt != 1 || !run.FireTime.Equal(first) || !run.ScheduledFor.Equal(first) {
		t.Fatalf("cron execution changed fire identity: %+v", run)
	}
	assertAttempt(t, id, 1, "SUCCEEDED", "", nil, 0)
}
