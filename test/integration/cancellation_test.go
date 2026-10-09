//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

// Report runtime completion before closing the reader, so drain assertions do
// not include Kafka's group-leave latency. Dependencies outlive result cleanup.
func controlledConsumer(t *testing.T, topic string, workerStore *store.Store, client *http.Client, dlq *kafka.Writer, options executorlib.Options) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	reader := kafkaReader(topic, "controlled-"+uuid.NewString())
	done := make(chan error, 1)
	go func() {
		done <- executorlib.Consume(ctx, reader, workerStore, client, dlq, logger, options)
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
	return stop, done
}

func awaitDrain(t *testing.T, done <-chan error, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("drain result = %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("shutdown exceeded its drain and cleanup bound")
	}
}

func TestHeartbeatReturnsOnlyOwnedRunningCancellations(t *testing.T) {
	worker, other := registeredWorker(t, time.Minute), registeredWorker(t, time.Minute)
	owned, healthy, foreign := queuedDispatch(t, store.Job{}), queuedDispatch(t, store.Job{}), queuedDispatch(t, store.Job{})
	startAttempt(t, owned, worker)
	startAttempt(t, healthy, worker)
	startAttempt(t, foreign, other)
	for _, message := range []store.DispatchMessage{owned, foreign} {
		if _, err := data.CancelRun(context.Background(), message.RunID); err != nil {
			t.Fatal(err)
		}
	}
	result, err := data.HeartbeatWorker(context.Background(), worker, time.Minute)
	if err != nil || !result.Renewed || len(result.Cancellations) != 1 || result.Cancellations[0] != (store.Cancellation{RunID: owned.RunID, Attempt: 1}) {
		t.Fatalf("heartbeat cancellations = %+v, %v", result, err)
	}
	for _, item := range []struct {
		message store.DispatchMessage
		worker  string
	}{{owned, worker}, {foreign, other}} {
		if _, err := data.FailRun(context.Background(), item.message.RunID, 1, item.worker, store.Failure{Error: "cancelled"}); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := data.SucceedRun(context.Background(), healthy.RunID, 1, worker); err != nil || !ok {
		t.Fatalf("healthy completion = %v, %v", ok, err)
	}
	expireWorker(t, worker)
	result, err = data.HeartbeatWorker(context.Background(), worker, time.Minute)
	if err != nil || result.Renewed || len(result.Cancellations) != 0 {
		t.Fatalf("expired heartbeat = %+v, %v", result, err)
	}
}

func TestCooperativeCancellationThroughAPIAndHeartbeat(t *testing.T) {
	sleep := queuedDispatch(t, store.Job{Payload: json.RawMessage(`{"handler":"sleep","duration_ms":60000}`), TimeoutMS: 120000, MaxAttempts: 1})
	pending, err := data.RunNow(context.Background(), sleep.JobID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = data.CancelRun(context.Background(), pending.ID) })
	queued := queuedDispatch(t, store.Job{})
	for _, id := range []string{pending.ID, queued.RunID} {
		response := perform(api, http.MethodPost, "/v1/runs/"+id+"/cancel", nil, apiKey, "")
		if response.Code != http.StatusOK {
			t.Fatalf("cancel response = %d, %s", response.Code, response.Body.String())
		}
		waitForRun(t, id, "CANCELLED")
	}
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumerWithOptions(t, topic, "cancel-handler-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, fastWorkerOptions(1))
	publishDispatch(t, writer, queued)
	publishDispatch(t, writer, sleep)
	waitForRun(t, sleep.RunID, "RUNNING")
	response := perform(api, http.MethodPost, "/v1/runs/"+sleep.RunID+"/cancel", nil, apiKey, "")
	if response.Code != http.StatusOK {
		t.Fatalf("cancel response = %d, %s", response.Code, response.Body.String())
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		run, err := data.GetRun(ctx, sleep.RunID)
		if err != nil {
			return err
		}
		if run.Status != "CANCELLED" || run.Attempt != 1 {
			return fmt.Errorf("heartbeat did not cancel sleep: %+v", run)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertAttempt(t, sleep.RunID, 1, "CANCELLED", "job cancellation requested", nil, 0)
	var attempts int
	if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text IN ($1,$2)`, pending.ID, queued.RunID).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("unstarted cancellations have attempt history: %d, %v", attempts, err)
	}
	assertEmptyTopic(t, dlqTopic)
}

func TestCancelCompletionRaces(t *testing.T) {
	worker := registeredWorker(t, time.Minute)
	for _, fail := range []bool{false, true} {
		for range 12 {
			message := queuedDispatch(t, store.Job{MaxAttempts: 1})
			startAttempt(t, message, worker)
			start := make(chan struct{})
			cancelled := make(chan error, 1)
			completed := make(chan error, 1)
			go func() {
				<-start
				_, err := data.CancelRun(context.Background(), message.RunID)
				cancelled <- err
			}()
			go func() {
				<-start
				if fail {
					run, err := data.FailRun(context.Background(), message.RunID, 1, worker, store.Failure{Error: "permanent", Permanent: true})
					if err == nil && run.ID == "" {
						err = errors.New("completion was not applied")
					}
					completed <- err
				} else {
					ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker)
					if err == nil && !ok {
						err = errors.New("completion was not applied")
					}
					completed <- err
				}
			}()
			close(start)
			cancelErr, completionErr := <-cancelled, <-completed
			if completionErr != nil || (cancelErr != nil && !errors.Is(cancelErr, store.ErrInvalidTransition)) {
				t.Fatalf("cancel/completion errors = %v, %v", cancelErr, completionErr)
			}
			status := "CANCELLED"
			if cancelErr != nil {
				status = "SUCCEEDED"
				if fail {
					status = "DEAD"
				}
			}
			run := waitForRun(t, message.RunID, status)
			assertAttempt(t, run.ID, 1, status, run.LastError, nil, 0)
		}
	}
}

func TestShutdownDrainsWithHeartbeatsAndStopsClaiming(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	releaseJob := sync.OnceFunc(func() { close(release) })
	defer releaseJob()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	payload, _ := json.Marshal(map[string]string{"url": server.URL})
	message := queuedDispatch(t, store.Job{Type: "http", Payload: payload})
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	o := fastWorkerOptions(1)
	o.HeartbeatInterval, o.LeaseDuration, o.SafetyMargin, o.DrainTimeout = 50*time.Millisecond, 900*time.Millisecond, 100*time.Millisecond, 3*time.Second
	stop, done := controlledConsumer(t, topic, data, executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, o)
	publishDispatch(t, writer, message)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("HTTP execution did not start")
	}
	run := waitForRun(t, message.RunID, "RUNNING")
	var before time.Time
	if err := database.QueryRow(`SELECT last_heartbeat FROM workers WHERE id::text=$1`, run.WorkerID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	stop()
	// Wait beyond the original lease while the drain is still in progress.
	time.Sleep(1100 * time.Millisecond)
	var after time.Time
	var live bool
	if err := database.QueryRow(`SELECT last_heartbeat, lease_expires_at>clock_timestamp() FROM workers WHERE id::text=$1`, run.WorkerID).Scan(&after, &live); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) || !live {
		t.Fatal("heartbeats stopped during the drain")
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown cancelled the draining job: %v", err)
	default:
	}
	next := queuedDispatch(t, store.Job{})
	publishDispatch(t, writer, next)
	releaseJob()
	awaitDrain(t, done, 3*time.Second)
	completed := waitForRun(t, message.RunID, "SUCCEEDED")
	if completed.Attempt != 1 || completed.WorkerID != run.WorkerID {
		t.Fatalf("drained completion = %+v", completed)
	}
	waitForRun(t, next.RunID, "QUEUED")
	assertAttempt(t, message.RunID, 1, "SUCCEEDED", "", nil, 0)
}

func TestShutdownForcedReleaseConsumesAttemptAndHonorsCancel(t *testing.T) {
	job := store.Job{Payload: json.RawMessage(`{"handler":"sleep","duration_ms":60000}`), TimeoutMS: 120000, MaxAttempts: 3, BackoffBaseMS: 10, BackoffMaxMS: 20}
	retry := queuedDispatch(t, job)
	job.MaxAttempts = 1
	dead, cancelled := queuedDispatch(t, job), queuedDispatch(t, job)
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	o := fastWorkerOptions(3)
	o.DrainTimeout = 200 * time.Millisecond
	stop, done := controlledConsumer(t, topic, data, executorlib.HTTPClient(nil), dlq, o)
	for _, message := range []store.DispatchMessage{retry, dead, cancelled} {
		publishDispatch(t, writer, message)
		waitForRun(t, message.RunID, "RUNNING")
	}
	if _, err := data.CancelRun(context.Background(), cancelled.RunID); err != nil {
		t.Fatal(err)
	}
	stop()
	awaitDrain(t, done, 2*time.Second)
	run := waitForRun(t, retry.RunID, "PENDING")
	if run.Attempt != 2 || run.WorkerID != "" || run.StartedAt != nil || !run.FireTime.Equal(retry.FireTime) {
		t.Fatalf("shutdown retry = %+v", run)
	}
	assertAttempt(t, retry.RunID, 1, "FAILED", "executor shutdown drain expired", nil, 0)
	waitForRun(t, dead.RunID, "DEAD")
	assertAttempt(t, dead.RunID, 1, "DEAD", "executor shutdown drain expired", nil, 0)
	final := waitForRun(t, cancelled.RunID, "CANCELLED")
	if final.Attempt != 1 {
		t.Fatalf("shutdown cancellation spent a retry: %+v", final)
	}
	assertAttempt(t, cancelled.RunID, 1, "CANCELLED", final.LastError, nil, 0)
	letter := readDeadLetter(t, dlqTopic)
	if letter.Message == nil || letter.Message.RunID != dead.RunID || letter.Error != "executor shutdown drain expired" {
		t.Fatalf("shutdown DEAD letter = %+v", letter)
	}
}

func TestShutdownDatabaseConnectionLossLeavesRecoveryToWatcher(t *testing.T) {
	// Give this executor a separate login, then reject reconnects and terminate
	// its connections. The shared watcher/API store remains available for recovery.
	role := "shutdown_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := database.Exec(fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'integration'`, role)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = database.Exec(fmt.Sprintf(`DROP OWNED BY %s`, role))
		_, _ = database.Exec(fmt.Sprintf(`DROP ROLE %s`, role))
	})
	if _, err := database.Exec(fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO %s`, role)); err != nil {
		t.Fatal(err)
	}
	connectionURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	connectionURL.User = url.UserPassword(role, "integration")
	workerStore, err := store.New(context.Background(), connectionURL.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerStore.Close)
	var requests atomic.Int32
	entered, interrupted := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(entered)
			<-r.Context().Done()
			close(interrupted)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	payload, _ := json.Marshal(map[string]string{"url": server.URL})
	message := queuedDispatch(t, store.Job{Type: "http", Payload: payload, BackoffBaseMS: 10, BackoffMaxMS: 20})
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	o := fastWorkerOptions(1)
	o.DrainTimeout = 200 * time.Millisecond
	stop, done := controlledConsumer(t, topic, workerStore, executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, o)
	publishDispatch(t, writer, message)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("HTTP execution did not start")
	}
	run := waitForRun(t, message.RunID, "RUNNING")
	if _, err := database.Exec(fmt.Sprintf(`ALTER ROLE %s NOLOGIN`, role)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename=$1`, role); err != nil {
		t.Fatal(err)
	}
	stop()
	awaitDrain(t, done, 2*time.Second)
	select {
	case <-interrupted:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not interrupt the HTTP request")
	}
	unpersisted := waitForRun(t, message.RunID, "RUNNING")
	if unpersisted.Attempt != 1 || unpersisted.FinishedAt != nil || unpersisted.LastError != "" {
		t.Fatalf("failed cleanup changed the run: %+v", unpersisted)
	}
	expireWorker(t, run.WorkerID)
	startConsumerWithOptions(t, topic, "shutdown-recovery-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, fastWorkerOptions(1))
	startWatcher(t, writer, dlq, fastWatcherOptions())
	recovered := waitForRun(t, message.RunID, "SUCCEEDED")
	if recovered.Attempt != 2 || recovered.WorkerID == run.WorkerID || requests.Load() != 2 {
		t.Fatalf("shutdown recovery = %+v, requests=%d", recovered, requests.Load())
	}
	assertAttempt(t, message.RunID, 1, "FAILED", "worker lease expired", nil, 0)
	assertAttempt(t, message.RunID, 2, "SUCCEEDED", "", nil, 0)
}
