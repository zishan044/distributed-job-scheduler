//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func TestPauseResumeAPI(t *testing.T) {
	first := databaseNow(t).Add(-time.Hour).Truncate(time.Microsecond)
	job := dueCronJob(t, "fire_once", first)
	at := first.Add(2 * time.Hour)
	oneShot, err := data.CreateJob(context.Background(), store.Job{
		Name: "pause-one-shot-" + uuid.NewString(), Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
	}, &at)
	if err != nil {
		t.Fatal(err)
	}
	deleted := dueCronJob(t, "fire_once", first)
	if _, err := database.Exec(`UPDATE jobs SET deleted_at=now() WHERE id::text=$1`, deleted.ID); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"pause", "resume"} {
		for _, tc := range []struct {
			id, token string
			status    int
		}{
			{job.ID, "", http.StatusUnauthorized},
			{"malformed", apiKey, http.StatusBadRequest},
			{uuid.NewString(), apiKey, http.StatusNotFound},
			{oneShot.ID, apiKey, http.StatusConflict},
			{deleted.ID, apiKey, http.StatusConflict},
		} {
			response := perform(api, http.MethodPost, "/v1/jobs/"+tc.id+"/"+action, nil, tc.token, "")
			if response.Code != tc.status {
				t.Fatalf("%s rejected job with %d %s; want %d", action, response.Code, response.Body.String(), tc.status)
			}
		}
		for range 2 {
			response := perform(api, http.MethodPost, "/v1/jobs/"+job.ID+"/"+action, nil, apiKey, "")
			var updated store.Job
			if response.Code != http.StatusOK {
				t.Fatalf("%s status/body = %d %s", action, response.Code, response.Body.String())
			}
			if err := json.Unmarshal(response.Body.Bytes(), &updated); err != nil {
				t.Fatal(err)
			}
			if updated.ID != job.ID || updated.Paused != (action == "pause") || updated.NextFireAt == nil || !updated.NextFireAt.Equal(first) {
				t.Fatalf("%s changed definition or cursor: %+v", action, updated)
			}
			persisted, err := data.GetJob(context.Background(), job.ID)
			if err != nil || persisted.Paused != updated.Paused || !persisted.NextFireAt.Equal(first) {
				t.Fatalf("%s did not persist its state: %+v, %v", action, persisted, err)
			}
		}
	}
}

func TestPauseResumePreservesMisfirePolicies(t *testing.T) {
	for _, policy := range []string{"fire_once", "skip", "fire_all"} {
		t.Run(policy, func(t *testing.T) {
			now := databaseNow(t)
			first := now.Truncate(time.Minute).Add(-3 * time.Minute)
			job := dueCronJob(t, policy, first)
			paused, err := data.SetJobPaused(context.Background(), job.ID, true)
			if err != nil || !paused.Paused || !paused.NextFireAt.Equal(first) {
				t.Fatalf("pause = %+v, %v", paused, err)
			}
			if count, err := data.MaterializeCron(context.Background(), 100); err != nil || count != 0 {
				t.Fatalf("paused job materialized: %d, %v", count, err)
			}
			resumed, err := data.SetJobPaused(context.Background(), job.ID, false)
			if err != nil || resumed.Paused || !resumed.NextFireAt.Equal(first) || resumed.MisfirePolicy != policy {
				t.Fatalf("resume = %+v, %v", resumed, err)
			}
			count, err := data.MaterializeCron(context.Background(), 100)
			if err != nil || (policy == "fire_once" && count != 1) || (policy == "skip" && count != 0) ||
				(policy == "fire_all" && (count < 4 || count > 5)) {
				t.Fatalf("%s resume materialized %d fires: %v", policy, count, err)
			}
		})
	}
}

func TestPausedCronAllowsExistingWorkAndRunNow(t *testing.T) {
	first := databaseNow(t).Truncate(time.Minute).Add(-3 * time.Minute)
	job := dueCronJob(t, "fire_all", first)
	if _, err := data.MaterializeCron(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
	if err != nil || len(runs) < 3 {
		t.Fatalf("materialized runs = %+v, %v", runs, err)
	}
	worker := registeredWorker(t, time.Minute)
	for _, run := range runs[1:3] {
		if ok, err := data.QueueRun(context.Background(), run.ID); err != nil || !ok {
			t.Fatalf("queue before pause = %v, %v", ok, err)
		}
	}
	if ok, err := data.StartRun(context.Background(), runs[2].ID, 1, worker); err != nil || !ok {
		t.Fatalf("start before pause = %v, %v", ok, err)
	}
	if _, err := data.SetJobPaused(context.Background(), job.ID, true); err != nil {
		t.Fatal(err)
	}
	for i, status := range []string{"PENDING", "QUEUED", "RUNNING"} {
		run := waitForRun(t, runs[i].ID, status)
		if run.CancelRequested {
			t.Fatalf("pause requested run cancellation: %+v", run)
		}
	}
	response := perform(api, http.MethodPost, "/v1/jobs/"+job.ID+"/run-now", nil, apiKey, "")
	var immediate store.Run
	if response.Code != http.StatusCreated {
		t.Fatalf("RunNow while paused = %d %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &immediate); err != nil {
		t.Fatal(err)
	}
	for i, run := range []store.Run{runs[0], runs[1], runs[2], immediate} {
		if i == 0 || i == 3 {
			if ok, err := data.QueueRun(context.Background(), run.ID); err != nil || !ok {
				t.Fatalf("queue while paused = %v, %v", ok, err)
			}
		}
		if i != 2 {
			if ok, err := data.StartRun(context.Background(), run.ID, 1, worker); err != nil || !ok {
				t.Fatalf("start while paused = %v, %v", ok, err)
			}
		}
		if ok, err := data.SucceedRun(context.Background(), run.ID, 1, worker); err != nil || !ok {
			t.Fatalf("complete while paused = %v, %v", ok, err)
		}
	}
}

func TestPauseMaterializationRace(t *testing.T) {
	for range 12 {
		first := databaseNow(t).Add(-time.Hour).Truncate(time.Microsecond)
		job := dueCronJob(t, "fire_once", first)
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		type materialization struct {
			count int
			err   error
		}
		start, paused, done := make(chan struct{}), make(chan error, 1), make(chan materialization, 1)
		go func() {
			<-start
			_, err := data.SetJobPaused(ctx, job.ID, true)
			paused <- err
		}()
		go func() {
			<-start
			count, err := data.MaterializeCron(ctx, 100)
			done <- materialization{count, err}
		}()
		close(start)
		pauseErr, result := <-paused, <-done
		stop()
		if pauseErr != nil || result.err != nil || result.count < 0 || result.count > 1 {
			t.Fatalf("pause/materialize race = %v, %+v", pauseErr, result)
		}
		updated, err := data.GetJob(context.Background(), job.ID)
		if err != nil || !updated.Paused || updated.NextFireAt == nil {
			t.Fatalf("race did not leave job paused: %+v, %v", updated, err)
		}
		runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
		if err != nil || len(runs) != result.count {
			t.Fatalf("race lost materialized runs: %+v, %+v, %v", runs, result, err)
		}
		if (result.count == 0 && !updated.NextFireAt.Equal(first)) ||
			(result.count == 1 && (!runs[0].FireTime.Equal(first) || !updated.NextFireAt.After(first))) {
			t.Fatalf("race corrupted cursor or fire identity: %+v, %+v", updated, runs)
		}
		if count, err := data.MaterializeCron(context.Background(), 100); err != nil || count != 0 {
			t.Fatalf("cron materialized after pause returned: %d, %v", count, err)
		}
	}
}
