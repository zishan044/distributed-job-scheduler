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
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	apiwire "github.com/zishan044/distributed-job-scheduler/pkg/api"
)

// Retention fixtures have old fire times and two attempts so pruning must use
// the run's finished_at and remove the entire history, including prior failures.
func retentionRun(t *testing.T, jobID, status string, finishedAt *time.Time) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := database.Exec(`INSERT INTO job_runs (id,job_id,fire_time,scheduled_for,status,attempt,finished_at)
		VALUES ($1,$2,$3,$3,$4,2,$5)`, id, jobID, time.Now().UTC().Add(-30*24*time.Hour), status, finishedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO run_attempts (run_id,attempt,finished_at,outcome)
		VALUES ($1,1,now()-interval '30 days','FAILED'),($1,2,$2,$3)`, id, finishedAt, status); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := database.Exec(`DELETE FROM run_attempts WHERE run_id::text=$1`, id); err != nil {
			t.Error(err)
		}
		if _, err := database.Exec(`DELETE FROM job_runs WHERE id::text=$1`, id); err != nil {
			t.Error(err)
		}
	})
	return id
}

func TestPruneRunsEligibilityAndHistory(t *testing.T) {
	ctx := context.Background()
	definition := store.Job{Name: "retention-" + uuid.NewString(), Type: "handler",
		Payload: json.RawMessage(`{"handler":"noop"}`), CronExpr: "* * * * *", Paused: true,
		IdempotencyKey: uuid.NewString()}
	job, err := data.CreateJob(ctx, definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	requests := []apiwire.CreateJobRequest{{Name: "retention-batch", Type: "handler",
		Payload: definition.Payload, CronExpr: definition.CronExpr, Paused: true}}
	key := uuid.NewString()
	batch, _, err := data.CreateJobs(ctx, requests, key)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := data.DeleteJob(ctx, batch[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	now := databaseNow(t)
	old, recent, future := now.Add(-8*24*time.Hour), now.Add(-7*24*time.Hour+time.Minute), now.Add(time.Hour)
	var expired []string
	for _, status := range []string{"SUCCEEDED", "FAILED", "CANCELLED", "DEAD"} {
		expired = append(expired, retentionRun(t, job.ID, status, &old))
	}
	expired = append(expired, retentionRun(t, deleted.ID, "SUCCEEDED", &old))
	child, err := data.RetryRun(ctx, expired[3])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = data.CancelRun(ctx, child.ID) })
	kept := []string{child.ID}
	for _, status := range []string{"SUCCEEDED", "FAILED", "CANCELLED", "DEAD"} {
		kept = append(kept, retentionRun(t, job.ID, status, &recent),
			retentionRun(t, job.ID, status, nil), retentionRun(t, job.ID, status, &future))
	}
	// Even a stale finished_at on an active run must never make it eligible.
	for _, status := range []string{"PENDING", "QUEUED", "RUNNING"} {
		kept = append(kept, retentionRun(t, job.ID, status, &old))
	}
	before := make(map[string]store.Run)
	for _, id := range kept {
		before[id], err = data.GetRun(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
	}
	if count, err := data.PruneRuns(ctx, 100); err != nil || count != int64(len(expired)) {
		t.Fatalf("pruned = %d, %v; want %d", count, err, len(expired))
	}
	for _, id := range expired {
		if _, err := data.GetRun(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("expired run remains: %s, %v", id, err)
		}
		var attempts int
		if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text=$1`, id).Scan(&attempts); err != nil || attempts != 0 {
			t.Fatalf("orphaned attempts = %d, %v", attempts, err)
		}
	}
	for _, id := range kept {
		after, err := data.GetRun(ctx, id)
		if err != nil || !reflect.DeepEqual(before[id], after) {
			t.Fatalf("retention changed a retained run: %+v, %v", after, err)
		}
		if id != child.ID {
			var attempts int
			if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text=$1`, id).Scan(&attempts); err != nil || attempts != 2 {
				t.Fatalf("retained attempts = %d, %v", attempts, err)
			}
		}
	}
	if response := perform(api, http.MethodGet, "/v1/runs/"+expired[0], nil, apiKey, ""); response.Code != http.StatusNotFound {
		t.Fatalf("pruned run API response = %d", response.Code)
	}
	if replay, err := data.CreateJob(ctx, definition, nil); err != nil || !reflect.DeepEqual(job, replay) {
		t.Fatalf("single idempotency record changed: %+v, %v", replay, err)
	}
	if replay, replayed, err := data.CreateJobs(ctx, requests, key); err != nil || !replayed || !reflect.DeepEqual(replay[0], deleted) {
		t.Fatalf("batch idempotency record changed: %+v, %v, %v", replay, replayed, err)
	}
	if count, err := data.PruneRuns(ctx, 100); err != nil || count != 0 {
		t.Fatalf("repeated pruning = %d, %v", count, err)
	}
}

func TestPruneRunsConcurrentBatchesSkipLocks(t *testing.T) {
	job := dueCronJob(t, "fire_once", databaseNow(t).Add(time.Hour))
	old := databaseNow(t).Add(-8 * 24 * time.Hour)
	held := retentionRun(t, job.ID, "DEAD", &old)
	for range 8 {
		retentionRun(t, job.ID, "SUCCEEDED", &old)
	}
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(`SELECT id FROM job_runs WHERE id::text=$1 FOR UPDATE`, held); err != nil {
		t.Fatal(err)
	}
	type result struct {
		count int64
		err   error
	}
	done, start := make(chan result, 2), make(chan struct{})
	for range 2 {
		go func() {
			<-start
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			count, err := data.PruneRuns(ctx, 2)
			done <- result{count, err}
		}()
	}
	close(start)
	for range 2 {
		if result := <-done; result.err != nil || result.count != 2 {
			t.Fatalf("concurrent bounded pruning = %+v", result)
		}
	}
	if count, err := data.PruneRuns(context.Background(), 0); err != nil || count != 1 {
		t.Fatalf("minimum batch = %d, %v", count, err)
	}
	if count, err := data.PruneRuns(context.Background(), 100); err != nil || count != 3 {
		t.Fatalf("remaining unlocked batch = %d, %v", count, err)
	}
	if _, err := data.GetRun(context.Background(), held); err != nil {
		t.Fatalf("locked run was removed: %v", err)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if count, err := data.PruneRuns(context.Background(), 100); err != nil || count != 1 {
		t.Fatalf("released batch = %d, %v", count, err)
	}
}

func TestPruneRunsFailureRollsBackAttempts(t *testing.T) {
	job := dueCronJob(t, "fire_once", databaseNow(t).Add(time.Hour))
	old := databaseNow(t).Add(-8 * 24 * time.Hour)
	id := retentionRun(t, job.ID, "DEAD", &old)
	if _, err := database.Exec(`CREATE TABLE retention_test_blocker (run_id uuid REFERENCES job_runs(id))`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = database.Exec(`DROP TABLE IF EXISTS retention_test_blocker`) })
	if _, err := database.Exec(`INSERT INTO retention_test_blocker VALUES ($1)`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := data.PruneRuns(context.Background(), 100); err == nil {
		t.Fatal("forced failure committed pruning")
	}
	if _, err := data.GetRun(context.Background(), id); err != nil {
		t.Fatalf("failed cleanup removed run: %v", err)
	}
	var attempts int
	if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text=$1`, id).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("failed cleanup lost attempts: %d, %v", attempts, err)
	}
	if _, err := database.Exec(`DROP TABLE retention_test_blocker`); err != nil {
		t.Fatal(err)
	}
	if count, err := data.PruneRuns(context.Background(), 100); err != nil || count != 1 {
		t.Fatalf("cleanup after failure = %d, %v", count, err)
	}
}

func TestWatcherRetentionDrainsStartupBacklog(t *testing.T) {
	job := dueCronJob(t, "fire_once", databaseNow(t).Add(time.Hour))
	old := databaseNow(t).Add(-8 * 24 * time.Hour)
	for range 5 {
		retentionRun(t, job.ID, "SUCCEEDED", &old)
	}
	writer, dlq := kafkaWriter(createKafkaTopic(t)), kafkaWriter(createKafkaTopic(t))
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	options := fastWatcherOptions()
	options.BatchSize = 2
	// The hour-long interval proves multiple batches drain at startup.
	startWatcher(t, writer, dlq, options)
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		runs, err := data.ListJobRuns(ctx, job.ID, 10, 0)
		if err == nil && len(runs) != 0 {
			return fmt.Errorf("startup backlog still contains %d runs", len(runs))
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
