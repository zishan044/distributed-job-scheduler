//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	apiwire "github.com/zishan044/distributed-job-scheduler/pkg/api"
)

func TestCronCreationDefaultsAndReplay(t *testing.T) {
	for _, batch := range []bool{false, true} {
		path := "/v1/jobs"
		if batch {
			path += "/batch"
		}
		t.Run(path, func(t *testing.T) {
			requests := batchRequests(3)
			for i := range requests {
				requests[i].ScheduledFor = nil
				requests[i].CronExpr = "0 9 * * *"
			}
			requests[1].Timezone = "Asia/Dhaka"
			requests[1].Paused = true
			requests[2].NextFireAt = timePtr(time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC))
			key := uuid.NewString()
			var jobs []apiwire.Job
			for _, request := range requests {
				body, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				if batch {
					body = batchBody(t, requests)
				}
				response := perform(api, http.MethodPost, path, body, apiKey, key)
				if response.Code != http.StatusCreated {
					t.Fatalf("create status/body = %d %s", response.Code, response.Body.String())
				}
				if batch {
					jobs = decodeBatch(t, response, http.StatusCreated)
				} else {
					var job apiwire.Job
					if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
						t.Fatal(err)
					}
					jobs = append(jobs, job)
				}
				replay := perform(api, http.MethodPost, path, body, apiKey, key)
				if replay.Code != http.StatusOK || replay.Body.String() != response.Body.String() {
					t.Fatalf("replay changed job or initial fire: %d %s", replay.Code, replay.Body.String())
				}
				if batch {
					break
				}
				key = uuid.NewString()
			}
			assertBatchRuns(t, jobs, requests)
			for i, job := range jobs {
				cleanupCronJob(t, job.ID)
				if job.NextFireAt == nil || job.MisfirePolicy != "fire_once" {
					t.Fatalf("missing cron defaults: %+v", job)
				}
				if requests[i].NextFireAt != nil {
					if !job.NextFireAt.Equal(*requests[i].NextFireAt) {
						t.Fatalf("override changed: %+v", job)
					}
					continue
				}
				location, err := time.LoadLocation(job.Timezone)
				if err != nil {
					t.Fatal(err)
				}
				created := job.CreatedAt.In(location)
				want := time.Date(created.Year(), created.Month(), created.Day(), 9, 0, 0, 0, location)
				if !want.After(created) {
					want = want.AddDate(0, 0, 1)
				}
				if !job.NextFireAt.Equal(want) || job.Paused != requests[i].Paused {
					t.Fatalf("next fire = %s, paused = %v; want %s, %v", job.NextFireAt, job.Paused, want, requests[i].Paused)
				}
				if batch && !job.CreatedAt.Equal(jobs[0].CreatedAt) {
					t.Fatal("batch jobs did not share a database clock anchor")
				}
			}
		})
	}
}

func TestCronCreationRejectsInvalidInputsAtomically(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, mutate := range []func(*apiwire.CreateJobRequest){
			func(r *apiwire.CreateJobRequest) { r.CronExpr = "@every 1m" },
			func(r *apiwire.CreateJobRequest) { r.CronExpr = "0 * * * * *" },
			func(r *apiwire.CreateJobRequest) { r.CronExpr = "CRON_TZ=UTC * * * * *" },
			func(r *apiwire.CreateJobRequest) { r.CronExpr = "0 0 31 2 *" },
			func(r *apiwire.CreateJobRequest) { r.Timezone = "Invalid/Zone" },
			func(r *apiwire.CreateJobRequest) { r.Timezone = "Local" },
			func(r *apiwire.CreateJobRequest) { r.MisfirePolicy = "unknown" },
		} {
			requests := batchRequests(2)
			requests[1].ScheduledFor = nil
			requests[1].CronExpr = "* * * * *"
			mutate(&requests[1])
			body, err := json.Marshal(requests[1])
			if err != nil {
				t.Fatal(err)
			}
			path := "/v1/jobs"
			if batch {
				path += "/batch"
				body = batchBody(t, requests)
			}
			key := uuid.NewString()
			response := perform(api, http.MethodPost, path, body, apiKey, key)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid cron create status/body = %d %s", response.Code, response.Body.String())
			}
			var count int
			if err := database.QueryRowContext(context.Background(), `SELECT count(*) FROM jobs WHERE name IN ($1,$2)`,
				requests[0].Name, requests[1].Name).Scan(&count); err != nil || count != 0 {
				t.Fatalf("invalid create persisted jobs: %d, %v", count, err)
			}
			if err := database.QueryRowContext(context.Background(), `SELECT count(*) FROM job_batches WHERE idempotency_key=$1`, key).Scan(&count); err != nil || count != 0 {
				t.Fatalf("invalid batch reserved key: %d, %v", count, err)
			}
		}
	}
}
