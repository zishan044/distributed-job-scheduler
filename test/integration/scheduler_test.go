//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/internal/store/migrations"
)

const apiKey = "integration-test-key"

var (
	data         *store.Store
	cache        *redis.Client
	api          http.Handler
	database     *sql.DB
	kafkaBrokers []string
	databaseURL  string
	logger       = slog.New(slog.NewTextHandler(io.Discard, nil))
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
	defer func() { _ = postgres.Terminate(context.Background()) }()

	postgresURL, err := containerAddress(ctx, postgres, "5432/tcp", "postgres://scheduler:scheduler@")
	if err != nil {
		return setupFailure("get postgres address", err)
	}
	postgresURL += "/scheduler?sslmode=disable"
	databaseURL = postgresURL
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
	defer func() { _ = redisContainer.Terminate(context.Background()) }()
	redisAddr, err := containerAddress(ctx, redisContainer, "6379/tcp", "")
	if err != nil {
		return setupFailure("get redis address", err)
	}
	cache = redis.NewClient(&redis.Options{Addr: redisAddr})
	defer cache.Close()
	if err := waitFor(ctx, func() error { return cache.Ping(ctx).Err() }); err != nil {
		return setupFailure("connect to redis", err)
	}
	kafka, err := kafkacontainer.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		return setupFailure("start kafka container", err)
	}
	defer func() { _ = kafka.Terminate(context.Background()) }()
	kafkaBrokers, err = kafka.Brokers(ctx)
	if err != nil {
		return setupFailure("get kafka brokers", err)
	}
	api = httpserver.NewAPIRouter(logger, data, cache, apiKey, data.Metrics.Registry)

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
	starts := metric(t, "start_latency_seconds", "").GetHistogram().GetSampleCount()
	successes := counterValue(t, "executions_total", "succeeded")
	stale := counterValue(t, "stale_completions_total", "")
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
	workerID := registeredWorker(t, 5*time.Minute)

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
	if got := metric(t, "start_latency_seconds", "").GetHistogram().GetSampleCount(); got != starts+1 {
		t.Fatalf("accepted start samples = %d, want %d", got, starts+1)
	}
	assertCounter(t, "executions_total", "succeeded", successes+1)
	assertCounter(t, "stale_completions_total", "", stale+2)
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

	workerID := registeredWorker(t, 5*time.Minute)
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
	createdCount := counterValue(t, "jobs_created_total", "")
	redisErrors := counterValue(t, "redis_errors_total", "")
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
	assertCounter(t, "jobs_created_total", "", createdCount+1)
	assertCounter(t, "redis_errors_total", "", redisErrors)

	unavailable := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		MaxRetries:   -1,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
	})
	defer unavailable.Close()
	fallbackAPI := httpserver.NewAPIRouter(logger, data, unavailable, apiKey, data.Metrics.Registry)
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
	assertCounter(t, "jobs_created_total", "", createdCount+1)
	assertCounter(t, "redis_errors_total", "", redisErrors+2)
}

func TestDispatchPublishFailureRollsBack(t *testing.T) {
	claimed := counterValue(t, "watcher_claimed_total", "")
	samples := metric(t, "dispatch_latency_seconds", "").GetHistogram().GetSampleCount()
	job, err := data.CreateJob(context.Background(), store.Job{
		Name: "dispatch-rollback-" + uuid.NewString(), Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
	}, timePtr(time.Now().Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	runs, err := data.ListJobRuns(context.Background(), job.ID, 1, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ListJobRuns() = %d, %v", len(runs), err)
	}
	wantErr := errors.New("publish failed")
	if _, err := data.DispatchDue(context.Background(), 10, func([]store.DispatchMessage) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("DispatchDue error = %v; want %v", err, wantErr)
	}
	run, err := data.GetRun(context.Background(), runs[0].ID)
	if err != nil || run.Status != "PENDING" {
		t.Fatalf("after publish failure run = %+v, %v; want PENDING", run, err)
	}
	assertCounter(t, "watcher_claimed_total", "", claimed)
	if got := metric(t, "dispatch_latency_seconds", "").GetHistogram().GetSampleCount(); got != samples {
		t.Fatalf("rolled-back dispatch added %d samples", got-samples)
	}
	if _, err := data.CancelRun(context.Background(), runs[0].ID); err != nil {
		t.Fatal(err)
	}
}

func TestScheduledJobDispatchesAndExecutes(t *testing.T) {
	scheduledFor := time.Now().Add(3 * time.Second)
	_, err := data.CreateJob(context.Background(), store.Job{
		Name: "e2e-" + uuid.NewString(), Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
	}, &scheduledFor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topic := createKafkaTopic(t)
	writer := kafkaWriter(topic)
	defer writer.Close()
	reader := kafkaReader(topic, "e2e-"+uuid.NewString())
	defer reader.Close()
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = data.DispatchDue(ctx, 100, func(batch []store.DispatchMessage) error {
					messages := make([]kafkago.Message, 0, len(batch))
					for _, item := range batch {
						value, err := json.Marshal(item)
						if err != nil {
							return err
						}
						messages = append(messages, kafkago.Message{Key: []byte(item.RunID), Value: value})
					}
					return writer.WriteMessages(ctx, messages...)
				})
			}
		}
	}()

	message, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var dispatch store.DispatchMessage
	if err := json.Unmarshal(message.Value, &dispatch); err != nil {
		t.Fatal(err)
	}
	workerID := registeredWorker(t, 5*time.Minute)
	claimed, err := data.StartRuns(ctx, []store.DispatchMessage{dispatch}, workerID)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("StartRuns() = %d, %v; want one claim", len(claimed), err)
	}
	if err := reader.CommitMessages(ctx, message); err != nil {
		t.Fatal(err)
	}
	run, err := data.GetRun(ctx, dispatch.RunID)
	if err != nil || run.StartedAt == nil {
		t.Fatalf("run start = %+v, %v", run, err)
	}
	if delay := run.StartedAt.Sub(scheduledFor); delay < 0 || delay > 2*time.Second {
		t.Fatalf("start delay = %s; want 0-2s", delay)
	}
	if err := executorlib.Execute(ctx, executorlib.HTTPClient(nil), dispatch); err != nil {
		t.Fatal(err)
	}
	if ok, err := data.SucceedRun(ctx, dispatch.RunID, dispatch.Attempt, workerID); err != nil || !ok {
		t.Fatalf("SucceedRun() = %v, %v; want true, nil", ok, err)
	}
}

func TestDuplicateKafkaDeliveryHasOneStartClaim(t *testing.T) {
	_, err := data.CreateJob(context.Background(), store.Job{
		Name: "duplicate-" + uuid.NewString(), Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
	}, timePtr(time.Now().Add(-time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	topic := createKafkaTopic(t)
	writer := kafkaWriter(topic)
	defer writer.Close()
	reader := kafkaReader(topic, "duplicate-"+uuid.NewString())
	defer reader.Close()
	if _, err := data.DispatchDue(context.Background(), 10, func(batch []store.DispatchMessage) error {
		value, err := json.Marshal(batch[0])
		if err != nil {
			return err
		}
		return writer.WriteMessages(context.Background(),
			kafkago.Message{Key: []byte(batch[0].RunID), Value: value},
			kafkago.Message{Key: []byte(batch[0].RunID), Value: value},
		)
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var messages []store.DispatchMessage
	for _, message := range []kafkago.Message{first, second} {
		var dispatch store.DispatchMessage
		if err := json.Unmarshal(message.Value, &dispatch); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, dispatch)
	}
	claimed, err := data.StartRuns(ctx, messages, registeredWorker(t, 5*time.Minute))
	if err != nil || len(claimed) != 1 {
		t.Fatalf("StartRuns() = %d claims, %v; want one", len(claimed), err)
	}
}

func TestHTTPExecutionAndSSRFGuard(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = r.Method == http.MethodPost && r.Header.Get("X-Test") == "yes"
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	payload, _ := json.Marshal(map[string]any{"url": server.URL, "method": "POST", "headers": map[string]string{"X-Test": "yes"}, "body": "ok"})
	message := store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 1000}
	if err := executorlib.Execute(context.Background(), executorlib.HTTPClient([]string{"127.0.0.1"}), message); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("HTTP executor did not send the configured request")
	}
	if err := executorlib.Execute(context.Background(), executorlib.HTTPClient(nil), message); err == nil {
		t.Fatal("HTTP executor allowed a loopback target without an allowlist")
	}
}

func createKafkaTopic(t *testing.T) string {
	t.Helper()
	topic := "test-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Client.CreateTopics also refreshes the transport shared by test writers.
	client := &kafkago.Client{Addr: kafkago.TCP(kafkaBrokers...)}
	response, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	return topic
}

func kafkaWriter(topic string) *kafkago.Writer {
	return &kafkago.Writer{Addr: kafkago.TCP(kafkaBrokers...), Topic: topic, Balancer: &kafkago.Hash{}, RequiredAcks: kafkago.RequireAll}
}

func kafkaReader(topic, group string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{Brokers: kafkaBrokers, Topic: topic, GroupID: group, CommitInterval: 0})
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
