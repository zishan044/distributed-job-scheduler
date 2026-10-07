//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/internal/store/migrations"
)

const apiKey = "integration-test-key"

var (
	data       *store.Store
	cache      *redis.Client
	api        http.Handler
	database   *sql.DB
	logger     = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18.6-bookworm",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "scheduler",
				"POSTGRES_PASSWORD": "scheduler",
				"POSTGRES_DB":       "scheduler",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	if err != nil {
		return setupFailure("start postgres container", err)
	}
	defer postgres.Terminate(context.Background())

	postgresURL, err := containerAddress(ctx, postgres, "5432/tcp", "postgres://scheduler:scheduler@")
	if err != nil {
		return setupFailure("get postgres address", err)
	}
	postgresURL += "/scheduler?sslmode=disable"
	database, err = sql.Open("pgx", postgresURL)
	if err != nil {
		return setupFailure("open postgres", err)
	}
	defer database.Close()
	if err := waitFor(ctx, func() error { return database.PingContext(ctx) }); err != nil {
		return setupFailure("connect to postgres", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, database, migrations.FS)
	if err != nil {
		return setupFailure("create migration provider", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return setupFailure("apply migrations", err)
	}

	data, err = store.New(ctx, postgresURL)
	if err != nil {
		return setupFailure("create store", err)
	}
	defer data.Close()

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:8.10.2-trixie",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	if err != nil {
		return setupFailure("start redis container", err)
	}
	defer redisContainer.Terminate(context.Background())
	redisAddr, err := containerAddress(ctx, redisContainer, "6379/tcp", "")
	if err != nil {
		return setupFailure("get redis address", err)
	}
	cache = redis.NewClient(&redis.Options{Addr: redisAddr})
	defer cache.Close()
	if err := waitFor(ctx, func() error { return cache.Ping(ctx).Err() }); err != nil {
		return setupFailure("connect to redis", err)
	}
	api = httpserver.NewAPIRouter(logger, data, cache, apiKey)

	return m.Run()
}

func setupFailure(message string, err error) int {
	fmt.Fprintf(os.Stderr, "integration setup: %s: %v\n", message, err)
	return 1
}

func containerAddress(ctx context.Context, container testcontainers.Container, port string, prefix string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}
	return prefix + net.JoinHostPort(host, mappedPort.Port()), nil
}

func waitFor(ctx context.Context, check func() error) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := check(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func TestStoreStateTransitions(t *testing.T) {
	job, err := data.CreateJob(context.Background(), store.Job{
		Name:    "transitions-" + uuid.NewString(),
		Type:    "handler",
		Payload: json.RawMessage(`{"handler":"noop"}`),
	}, timePtr(time.Now().Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	runs, err := data.ListJobRuns(context.Background(), job.ID, 10, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ListJobRuns() = %d runs, err %v", len(runs), err)
	}
	runID := runs[0].ID
	workerID := uuid.NewString()

	if queued, err := data.QueueRun(context.Background(), runID); err != nil || !queued {
		t.Fatalf("QueueRun() = %v, %v; want true, nil", queued, err)
	}
	if queued, err := data.QueueRun(context.Background(), runID); err != nil || queued {
		t.Fatalf("duplicate QueueRun() = %v, %v; want false, nil", queued, err)
	}
	if started, err := data.StartRun(context.Background(), runID, runs[0].Attempt+1, workerID); err != nil || started {
		t.Fatalf("StartRun() with wrong attempt = %v, %v; want false, nil", started, err)
	}
	if started, err := data.StartRun(context.Background(), runID, runs[0].Attempt, workerID); err != nil || !started {
		t.Fatalf("StartRun() = %v, %v; want true, nil", started, err)
	}
	if started, err := data.StartRun(context.Background(), runID, runs[0].Attempt, workerID); err != nil || started {
		t.Fatalf("duplicate StartRun() = %v, %v; want false, nil", started, err)
	}
	if succeeded, err := data.SucceedRun(context.Background(), runID, runs[0].Attempt, uuid.NewString()); err != nil || succeeded {
		t.Fatalf("SucceedRun() with wrong worker = %v, %v; want false, nil", succeeded, err)
	}
	if succeeded, err := data.SucceedRun(context.Background(), runID, runs[0].Attempt, workerID); err != nil || !succeeded {
		t.Fatalf("SucceedRun() = %v, %v; want true, nil", succeeded, err)
	}
	if succeeded, err := data.SucceedRun(context.Background(), runID, runs[0].Attempt, workerID); err != nil || succeeded {
		t.Fatalf("duplicate SucceedRun() = %v, %v; want false, nil", succeeded, err)
	}
	if _, err := data.CancelRun(context.Background(), runID); err != store.ErrInvalidTransition {
		t.Fatalf("CancelRun() on succeeded run = %v; want ErrInvalidTransition", err)
	}

	var outcome string
	if err := database.QueryRow(`SELECT outcome FROM run_attempts WHERE run_id::text = $1`, runID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "SUCCEEDED" {
		t.Fatalf("attempt outcome = %q; want SUCCEEDED", outcome)
	}
}

func TestCancelStartRace(t *testing.T) {
	job, err := data.CreateJob(context.Background(), store.Job{
		Name:    "cancel-race-" + uuid.NewString(),
		Type:    "handler",
		Payload: json.RawMessage(`{"handler":"noop"}`),
	}, timePtr(time.Now().Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	runs, err := data.ListJobRuns(context.Background(), job.ID, 1, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ListJobRuns() = %d runs, err %v", len(runs), err)
	}
	if queued, err := data.QueueRun(context.Background(), runs[0].ID); err != nil || !queued {
		t.Fatalf("QueueRun() = %v, %v; want true, nil", queued, err)
	}

	workerID := uuid.NewString()
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var claimed bool
	var claimErr error
	var canceled store.Run
	var cancelErr error
	go func() {
		defer wait.Done()
		<-start
		claimed, claimErr = data.StartRun(context.Background(), runs[0].ID, runs[0].Attempt, workerID)
	}()
	go func() {
		defer wait.Done()
		<-start
		canceled, cancelErr = data.CancelRun(context.Background(), runs[0].ID)
	}()
	close(start)
	wait.Wait()
	if claimErr != nil {
		t.Fatal(claimErr)
	}
	if cancelErr != nil {
		t.Fatal(cancelErr)
	}

	final, err := data.GetRun(context.Background(), runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	switch final.Status {
	case "CANCELLED":
		if claimed || canceled.Status != "CANCELLED" {
			t.Fatalf("cancel won with inconsistent results: claimed=%v response=%s", claimed, canceled.Status)
		}
	case "RUNNING":
		if !claimed || !final.CancelRequested || canceled.Status != "RUNNING" || !canceled.CancelRequested {
			t.Fatalf("start won without recording cancel request: claimed=%v final=%+v response=%+v", claimed, final, canceled)
		}
	default:
		t.Fatalf("race ended in unexpected status %q", final.Status)
	}
}

func TestAPIEndpointsAndAuthentication(t *testing.T) {
	body := oneShotBody(t, "api-"+uuid.NewString(), time.Now().Add(time.Hour))
	unauthorized := perform(api, http.MethodPost, "/v1/jobs", body, "", "")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized POST status = %d; want 401", unauthorized.Code)
	}

	created := perform(api, http.MethodPost, "/v1/jobs", body, apiKey, "")
	if created.Code != http.StatusCreated {
		t.Fatalf("POST /jobs status = %d, body %s", created.Code, created.Body.String())
	}
	var job store.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.ID == "" {
		t.Fatal("created job has no ID")
	}

	gotJob := perform(api, http.MethodGet, "/v1/jobs/"+job.ID, nil, apiKey, "")
	if gotJob.Code != http.StatusOK {
		t.Fatalf("GET /jobs/{id} status = %d, body %s", gotJob.Code, gotJob.Body.String())
	}
	listed := perform(api, http.MethodGet, "/v1/jobs/"+job.ID+"/runs", nil, apiKey, "")
	if listed.Code != http.StatusOK {
		t.Fatalf("GET /jobs/{id}/runs status = %d, body %s", listed.Code, listed.Body.String())
	}

	createdRun := perform(api, http.MethodPost, "/v1/jobs/"+job.ID+"/run-now", nil, apiKey, "")
	if createdRun.Code != http.StatusCreated {
		t.Fatalf("POST /jobs/{id}/run-now status = %d, body %s", createdRun.Code, createdRun.Body.String())
	}
	var run store.Run
	if err := json.Unmarshal(createdRun.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	gotRun := perform(api, http.MethodGet, "/v1/runs/"+run.ID, nil, apiKey, "")
	if gotRun.Code != http.StatusOK {
		t.Fatalf("GET /runs/{id} status = %d, body %s", gotRun.Code, gotRun.Body.String())
	}
	canceled := perform(api, http.MethodPost, "/v1/runs/"+run.ID+"/cancel", nil, apiKey, "")
	if canceled.Code != http.StatusOK || !strings.Contains(canceled.Body.String(), `"CANCELLED"`) {
		t.Fatalf("POST /runs/{id}/cancel status/body = %d %s", canceled.Code, canceled.Body.String())
	}
}

func TestCreateIdempotencyUsesRedisAndDatabaseFallback(t *testing.T) {
	key := "idem-" + uuid.NewString()
	body := oneShotBody(t, "idempotent-"+uuid.NewString(), time.Now().Add(time.Hour))
	first := perform(api, http.MethodPost, "/v1/jobs", body, apiKey, key)
	if first.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, body %s", first.Code, first.Body.String())
	}
	var created store.Job
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	second := perform(api, http.MethodPost, "/v1/jobs", body, apiKey, key)
	if second.Code != http.StatusOK {
		t.Fatalf("cached duplicate status = %d, body %s", second.Code, second.Body.String())
	}
	var cached store.Job
	if err := json.Unmarshal(second.Body.Bytes(), &cached); err != nil {
		t.Fatal(err)
	}
	if cached.ID != created.ID {
		t.Fatalf("cached ID = %s; want %s", cached.ID, created.ID)
	}

	unavailable := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		MaxRetries:   -1,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
	})
	defer unavailable.Close()
	fallbackAPI := httpserver.NewAPIRouter(logger, data, unavailable, apiKey)
	fallback := perform(fallbackAPI, http.MethodPost, "/v1/jobs", body, apiKey, key)
	if fallback.Code != http.StatusCreated {
		t.Fatalf("database fallback status = %d, body %s", fallback.Code, fallback.Body.String())
	}
	var deduplicated store.Job
	if err := json.Unmarshal(fallback.Body.Bytes(), &deduplicated); err != nil {
		t.Fatal(err)
	}
	if deduplicated.ID != created.ID {
		t.Fatalf("database fallback ID = %s; want %s", deduplicated.ID, created.ID)
	}
	runs, err := data.ListJobRuns(context.Background(), created.ID, 10, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("idempotent create left %d runs, err %v; want one run", len(runs), err)
	}
}

func oneShotBody(t *testing.T, name string, scheduledFor time.Time) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"name":          name,
		"type":          "handler",
		"payload":       json.RawMessage(`{"handler":"noop"}`),
		"scheduled_for": scheduledFor.UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func perform(handler http.Handler, method, path string, body []byte, token, idempotencyKey string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	if body == nil {
		request.Body = http.NoBody
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func timePtr(value time.Time) *time.Time {
	return &value
}
