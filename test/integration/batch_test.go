//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	apiwire "github.com/zishan044/distributed-job-scheduler/pkg/api"
)

func batchRequests(count int) []apiwire.CreateJobRequest {
	at := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	prefix := "batch-" + uuid.NewString()
	jobs := make([]apiwire.CreateJobRequest, count)
	for i := range jobs {
		jobs[i] = apiwire.CreateJobRequest{
			Name: fmt.Sprintf("%s-%d", prefix, i), Type: "handler",
			Payload: json.RawMessage(`{"handler":"noop"}`), ScheduledFor: &at,
		}
	}
	return jobs
}

func batchBody(t *testing.T, jobs []apiwire.CreateJobRequest) []byte {
	t.Helper()
	body, err := json.Marshal(apiwire.CreateBatchRequest{Jobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func decodeBatch(t *testing.T, response *httptest.ResponseRecorder, status int) []apiwire.Job {
	t.Helper()
	if response.Code != status {
		t.Fatalf("batch status/body = %d %s; want %d", response.Code, response.Body.String(), status)
	}
	var jobs []apiwire.Job
	if err := json.Unmarshal(response.Body.Bytes(), &jobs); err != nil {
		t.Fatal(err)
	}
	return jobs
}

func assertBatchRuns(t *testing.T, jobs []apiwire.Job, requests []apiwire.CreateJobRequest) {
	t.Helper()
	if len(jobs) != len(requests) {
		t.Fatalf("batch returned %d jobs; want %d", len(jobs), len(requests))
	}
	for i, job := range jobs {
		if job.Name != requests[i].Name {
			t.Fatalf("batch job %d name = %q; want %q", i, job.Name, requests[i].Name)
		}
		runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		if at := requests[i].ScheduledFor; at != nil {
			if len(runs) != 1 || runs[0].Status != apiwire.StatusPending || runs[0].Attempt != 1 ||
				!runs[0].FireTime.Equal(*at) || !runs[0].ScheduledFor.Equal(*at) {
				t.Fatalf("job %s initial runs = %+v; want one pending run at %s", job.ID, runs, at)
			}
		} else if len(runs) != 0 {
			t.Fatalf("cron job %s has initial runs: %+v", job.ID, runs)
		}
	}
}

func TestBatchBoundsValidationAndDefaults(t *testing.T) {
	before := counterValue(t, "jobs_created_total", "")
	valid := batchRequests(2)
	unauthorized := perform(api, http.MethodPost, "/v1/jobs/batch", batchBody(t, valid), "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized batch status = %d", unauthorized.Code)
	}
	for _, body := range [][]byte{
		[]byte(`{`), []byte(`null`), []byte(`{}`), []byte(`{"jobs":null}`),
		batchBody(t, nil), batchBody(t, []apiwire.CreateJobRequest{}), batchBody(t, batchRequests(101)),
	} {
		response := perform(api, http.MethodPost, "/v1/jobs/batch", body, apiKey, "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid batch %s status/body = %d %s", body, response.Code, response.Body.String())
		}
	}
	for _, mutate := range []func(*apiwire.CreateJobRequest){
		func(r *apiwire.CreateJobRequest) { r.Name = "" },
		func(r *apiwire.CreateJobRequest) { r.Type = "unknown" },
		func(r *apiwire.CreateJobRequest) { r.MaxAttempts = -1 },
		func(r *apiwire.CreateJobRequest) { r.BackoffBaseMS = -1 },
		func(r *apiwire.CreateJobRequest) { r.BackoffMaxMS = 1 },
		func(r *apiwire.CreateJobRequest) { r.TimeoutMS = -1 },
		func(r *apiwire.CreateJobRequest) { r.ScheduledFor = nil },
		func(r *apiwire.CreateJobRequest) { r.CronExpr = "* * * * *" },
	} {
		requests := append([]apiwire.CreateJobRequest(nil), valid...)
		mutate(&requests[1])
		response := perform(api, http.MethodPost, "/v1/jobs/batch", batchBody(t, requests), apiKey, uuid.NewString())
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid job status/body = %d %s", response.Code, response.Body.String())
		}
	}
	var count int
	if err := database.QueryRow(`SELECT count(*) FROM jobs WHERE name=$1`, valid[0].Name).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid batches persisted first job: count=%d, err=%v", count, err)
	}
	assertCounter(t, "jobs_created_total", "", before)

	requests := batchRequests(apiwire.MaxBatchSize)
	jobs := decodeBatch(t, perform(api, http.MethodPost, "/v1/jobs/batch", batchBody(t, requests), apiKey, ""), http.StatusCreated)
	assertBatchRuns(t, jobs, requests)
	for _, job := range jobs {
		if job.Timezone != "UTC" || job.MisfirePolicy != "fire_once" || job.MaxAttempts != 5 ||
			job.BackoffBaseMS != 1000 || job.BackoffMaxMS != 300000 || job.TimeoutMS != 30000 {
			t.Fatalf("batch defaults = %+v", job)
		}
	}
	assertCounter(t, "jobs_created_total", "", before+apiwire.MaxBatchSize)
}

func TestBatchConcurrentReplayAndConflict(t *testing.T) {
	before := counterValue(t, "jobs_created_total", "")
	requests := batchRequests(2)
	key, body := uuid.NewString(), batchBody(t, requests)
	const callers = 8
	responses := make(chan *httptest.ResponseRecorder, callers)
	for range callers {
		go func() { responses <- perform(api, http.MethodPost, "/v1/jobs/batch", body, apiKey, key) }()
	}
	var original []apiwire.Job
	created := 0
	for range callers {
		response := <-responses
		if response.Code != http.StatusOK && response.Code != http.StatusCreated {
			t.Fatalf("concurrent batch status/body = %d %s", response.Code, response.Body.String())
		}
		if response.Code == http.StatusCreated {
			created++
		}
		jobs := decodeBatch(t, response, response.Code)
		if original == nil {
			original = jobs
		} else if !reflect.DeepEqual(jobs, original) {
			t.Fatalf("concurrent replay changed jobs: %+v; want %+v", jobs, original)
		}
	}
	if created != 1 {
		t.Fatalf("%d concurrent requests created jobs; want one", created)
	}
	assertBatchRuns(t, original, requests)
	assertCounter(t, "jobs_created_total", "", before+2)

	for _, changed := range [][]apiwire.CreateJobRequest{
		{requests[1], requests[0]}, requests[:1], append(append([]apiwire.CreateJobRequest(nil), requests...), batchRequests(1)[0]),
	} {
		response := perform(api, http.MethodPost, "/v1/jobs/batch", batchBody(t, changed), apiKey, key)
		if response.Code != http.StatusConflict {
			t.Fatalf("changed batch status/body = %d %s; want 409", response.Code, response.Body.String())
		}
		var failure apiwire.ErrorResponse
		if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil || failure.Error == "" {
			t.Fatalf("conflict error body = %s, err=%v", response.Body.String(), err)
		}
	}
	changed := append([]apiwire.CreateJobRequest(nil), requests...)
	changed[1].Payload = json.RawMessage(`{"handler":"sleep","duration_ms":1}`)
	response := perform(api, http.MethodPost, "/v1/jobs/batch", batchBody(t, changed), apiKey, key)
	if response.Code != http.StatusConflict {
		t.Fatalf("changed payload status/body = %d %s", response.Code, response.Body.String())
	}
	assertCounter(t, "jobs_created_total", "", before+2)
	assertBatchRuns(t, original, requests)
}

func TestBatchCanonicalReplayWithoutRedis(t *testing.T) {
	unavailable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	defer unavailable.Close()
	router := httpserver.NewAPIRouter(logger, data, unavailable, apiKey, data.Metrics.Registry)
	before := counterValue(t, "jobs_created_total", "")
	redisErrors := counterValue(t, "redis_errors_total", "")
	requests := batchRequests(1)
	requests[0].Payload = json.RawMessage(`{"handler":"noop","nested":{"b":2,"a":1}}`)
	key := uuid.NewString()
	original := decodeBatch(t, perform(router, http.MethodPost, "/v1/jobs/batch", batchBody(t, requests), apiKey, key), http.StatusCreated)
	requests[0].Payload = json.RawMessage(`{ "nested": { "a":1.0, "b":2 }, "handler":"noop" }`)
	replayed := decodeBatch(t, perform(router, http.MethodPost, "/v1/jobs/batch", batchBody(t, requests), apiKey, key), http.StatusOK)
	if !reflect.DeepEqual(replayed, original) {
		t.Fatalf("canonical replay = %+v; want %+v", replayed, original)
	}
	assertBatchRuns(t, replayed, requests)
	assertCounter(t, "jobs_created_total", "", before+1)
	assertCounter(t, "redis_errors_total", "", redisErrors)
	// No header means distinct creates; reusing a batch key for a single create
	// is independent because idempotency keys are scoped to their endpoint.
	unkeyed := decodeBatch(t, perform(router, http.MethodPost, "/v1/jobs/batch", batchBody(t, requests), apiKey, ""), http.StatusCreated)
	if unkeyed[0].ID == original[0].ID {
		t.Fatal("unkeyed batch reused a previous job")
	}
	singleBody, err := json.Marshal(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	single := perform(api, http.MethodPost, "/v1/jobs", singleBody, apiKey, key)
	if single.Code != http.StatusCreated {
		t.Fatalf("single create sharing batch key = %d %s", single.Code, single.Body.String())
	}
	assertCounter(t, "jobs_created_total", "", before+3)
}

func TestBatchPersistenceFailureRollsBackAndKeyCanRetry(t *testing.T) {
	before := counterValue(t, "jobs_created_total", "")
	requests := batchRequests(2)
	at := requests[1].ScheduledFor.Format(time.RFC3339Nano)
	// Force initial-run insertion to fail after the multi-row job insert succeeds.
	if _, err := database.Exec(fmt.Sprintf(`ALTER TABLE job_runs ADD CONSTRAINT batch_test_run_failure
		CHECK (fire_time <> TIMESTAMPTZ '%s') NOT VALID`, at)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := database.Exec(`ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS batch_test_run_failure`); err != nil {
			t.Error(err)
		}
	})
	key, body := uuid.NewString(), batchBody(t, requests)
	response := perform(api, http.MethodPost, "/v1/jobs/batch", body, apiKey, key)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("forced persistence failure status/body = %d %s", response.Code, response.Body.String())
	}
	var count int
	if err := database.QueryRow(`SELECT count(*) FROM jobs WHERE name IN ($1,$2)`, requests[0].Name, requests[1].Name).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed batch left jobs: count=%d, err=%v", count, err)
	}
	if err := database.QueryRow(`SELECT count(*) FROM job_batches WHERE idempotency_key=$1`, key).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed batch reserved key: count=%d, err=%v", count, err)
	}
	assertCounter(t, "jobs_created_total", "", before)
	if _, err := database.Exec(`ALTER TABLE job_runs DROP CONSTRAINT batch_test_run_failure`); err != nil {
		t.Fatal(err)
	}
	jobs := decodeBatch(t, perform(api, http.MethodPost, "/v1/jobs/batch", body, apiKey, key), http.StatusCreated)
	assertBatchRuns(t, jobs, requests)
	assertCounter(t, "jobs_created_total", "", before+2)
}

func TestBatchPreservesCronAndCustomSettings(t *testing.T) {
	requests := batchRequests(2)
	requests[0].MaxAttempts = 2
	requests[0].BackoffBaseMS, requests[0].BackoffMaxMS, requests[0].TimeoutMS = 10, 20, 100
	requests[1].CronExpr, requests[1].Timezone, requests[1].MisfirePolicy = "* * * * *", "Asia/Dhaka", "skip"
	requests[1].ScheduledFor = nil
	requests[1].NextFireAt = requests[0].ScheduledFor
	requests[1].Paused = true
	jobs := decodeBatch(t, perform(api, http.MethodPost, "/v1/jobs/batch", batchBody(t, requests), apiKey, ""), http.StatusCreated)
	assertBatchRuns(t, jobs, requests)
	if jobs[0].MaxAttempts != 2 || jobs[0].BackoffBaseMS != 10 || jobs[0].BackoffMaxMS != 20 || jobs[0].TimeoutMS != 100 ||
		jobs[1].CronExpr != requests[1].CronExpr || jobs[1].Timezone != "Asia/Dhaka" || jobs[1].MisfirePolicy != "skip" ||
		!jobs[1].Paused || jobs[1].NextFireAt == nil || !jobs[1].NextFireAt.Equal(*requests[1].NextFireAt) {
		t.Fatalf("custom settings = %+v", jobs)
	}
}
