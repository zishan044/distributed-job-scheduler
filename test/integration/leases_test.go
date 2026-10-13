//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	executorlib "github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/internal/watcher"
)

func registeredWorker(t *testing.T, lease time.Duration) string {
	t.Helper()
	id := uuid.NewString()
	if err := data.RegisterWorker(context.Background(), id, "integration", 10, lease); err != nil {
		t.Fatal(err)
	}
	return id
}

func expireWorker(t *testing.T, id string) {
	t.Helper()
	if _, err := database.Exec(`UPDATE workers SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id::text=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHeartbeatAndExpiredLeaseCannotRevive(t *testing.T) {
	id := registeredWorker(t, time.Second)
	var before time.Time
	if err := database.QueryRow(`SELECT last_heartbeat FROM workers WHERE id::text=$1`, id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if ok, err := data.HeartbeatWorker(context.Background(), id, 2*time.Second); err != nil || !ok.Renewed {
		t.Fatalf("renew = %v, %v", ok, err)
	}
	var after time.Time
	var capacity int
	var hostname string
	var live bool
	if err := database.QueryRow(`SELECT last_heartbeat, capacity, hostname, lease_expires_at>clock_timestamp() FROM workers WHERE id::text=$1`, id).Scan(&after, &capacity, &hostname, &live); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) || !live || capacity != 10 || hostname != "integration" {
		t.Fatalf("worker registration/renewal: heartbeat=%s, capacity=%d, hostname=%q, live=%v", after, capacity, hostname, live)
	}
	if err := data.RegisterWorker(context.Background(), id, "reused", 10, time.Minute); err == nil {
		t.Fatal("reused a worker identity")
	}
	expireWorker(t, id)
	if ok, err := data.HeartbeatWorker(context.Background(), id, time.Minute); err != nil || ok.Renewed {
		t.Fatalf("revive expired worker = %v, %v", ok, err)
	}

	id = registeredWorker(t, 200*time.Millisecond)
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(`SELECT id FROM workers WHERE id::text=$1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	type renewal struct {
		ok  store.Heartbeat
		err error
	}
	done := make(chan renewal, 1)
	go func() {
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		ok, err := data.HeartbeatWorker(ctx, id, time.Minute)
		done <- renewal{ok, err}
	}()
	time.Sleep(300 * time.Millisecond)
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || result.ok.Renewed {
		t.Fatalf("renewal after a lock wait revived the expired lease: %+v", result)
	}
}

func TestLeaseFencingAfterRunLockWait(t *testing.T) {
	for _, operation := range []string{"start", "succeed", "fail"} {
		t.Run(operation, func(t *testing.T) {
			message := queuedDispatch(t, store.Job{BackoffBaseMS: 10, BackoffMaxMS: 20})
			worker := registeredWorker(t, 150*time.Millisecond)
			if operation != "start" {
				startAttempt(t, message, worker)
			}
			lock, err := database.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Rollback() }()
			if _, err := lock.Exec(`SELECT id FROM job_runs WHERE id::text=$1 FOR UPDATE`, message.RunID); err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				applied bool
				err     error
			}
			done := make(chan outcome, 1)
			go func() {
				ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				var applied bool
				var err error
				switch operation {
				case "start":
					applied, err = data.StartRun(ctx, message.RunID, 1, worker)
				case "succeed":
					applied, err = data.SucceedRun(ctx, message.RunID, 1, worker)
				case "fail":
					var run store.Run
					run, err = data.FailRun(ctx, message.RunID, 1, worker, store.Failure{Error: "late"})
					applied = run.ID != ""
				}
				done <- outcome{applied, err}
			}()
			time.Sleep(250 * time.Millisecond)
			if err := lock.Rollback(); err != nil {
				t.Fatal(err)
			}
			result := <-done
			if result.applied || (operation == "start" && !errors.Is(result.err, store.ErrLeaseLost)) || (operation != "start" && result.err != nil) {
				t.Fatalf("expired %s applied after a lock wait: %+v", operation, result)
			}
			status := "RUNNING"
			if operation == "start" {
				status = "QUEUED"
			}
			run := waitForRun(t, message.RunID, status)
			if run.FinishedAt != nil || run.LastError != "" || run.Attempt != 1 {
				t.Fatalf("fenced operation changed run: %+v", run)
			}
			if operation != "start" {
				if _, err := data.Sweep(context.Background(), 100, 5*time.Minute); err != nil {
					t.Fatal(err)
				}
				waitForRun(t, message.RunID, "PENDING")
			}
		})
	}
}

func TestConcurrentSweepIsBoundedAndSkipsLockedRuns(t *testing.T) {
	zombies := counterValue(t, "zombies_recovered_total", "")
	retries := counterValue(t, "retries_total", "")
	failed := counterValue(t, "executions_total", "failed")
	dead := counterValue(t, "executions_total", "dead")
	cancelled := counterValue(t, "executions_total", "cancelled")
	crashed := registeredWorker(t, time.Minute)
	expected := make(map[string]string)
	var held store.DispatchMessage
	for _, status := range []string{"PENDING", "DEAD", "CANCELLED", "missing", "held"} {
		job := store.Job{MaxAttempts: 3, BackoffBaseMS: 10, BackoffMaxMS: 20}
		if status == "DEAD" || status == "CANCELLED" {
			job.MaxAttempts = 1
		}
		message := queuedDispatch(t, job)
		worker := crashed
		if status == "missing" {
			worker = registeredWorker(t, time.Minute)
		}
		startAttempt(t, message, worker)
		if status == "CANCELLED" {
			if _, err := data.CancelRun(context.Background(), message.RunID); err != nil {
				t.Fatal(err)
			}
		}
		if status == "missing" {
			if _, err := database.Exec(`DELETE FROM workers WHERE id::text=$1`, worker); err != nil {
				t.Fatal(err)
			}
			status = "PENDING"
		}
		if status == "held" {
			held = message
		} else {
			expected[message.RunID] = status
		}
	}
	liveMessage := queuedDispatch(t, store.Job{})
	liveWorker := registeredWorker(t, time.Minute)
	startAttempt(t, liveMessage, liveWorker)
	expireWorker(t, crashed)
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	if _, err := lock.Exec(`SELECT id FROM job_runs WHERE id::text=$1 FOR UPDATE`, held.RunID); err != nil {
		t.Fatal(err)
	}
	type sweep struct {
		result store.SweepResult
		err    error
	}
	done := make(chan sweep, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			result, err := data.Sweep(ctx, 2, 5*time.Minute)
			done <- sweep{result, err}
		}()
	}
	close(start)
	seen := make(map[string]bool)
	for range 2 {
		result := <-done
		if result.err != nil || len(result.result.Recovered) != 2 {
			t.Fatalf("bounded sweep = %+v, %v", result.result, result.err)
		}
		for _, recovered := range result.result.Recovered {
			run := recovered.Run
			if seen[run.ID] || run.Status != expected[run.ID] || run.ID != recovered.Message.RunID || !run.FireTime.Equal(recovered.Message.FireTime) {
				t.Fatalf("unexpected or duplicate recovery: %+v", recovered)
			}
			seen[run.ID] = true
			outcome := run.Status
			if run.Status == "PENDING" {
				outcome = "FAILED"
				if run.Attempt != 2 || run.WorkerID != "" {
					t.Fatalf("recovered retry = %+v", run)
				}
			}
			assertAttempt(t, run.ID, 1, outcome, "worker lease expired", nil, 0)
		}
	}
	waitForRun(t, held.RunID, "RUNNING")
	waitForRun(t, liveMessage.RunID, "RUNNING")
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	result, err := data.Sweep(context.Background(), 1, 5*time.Minute)
	if err != nil || len(result.Recovered) != 1 || result.Recovered[0].Run.ID != held.RunID {
		t.Fatalf("unlocked recovery = %+v, %v", result, err)
	}
	if ok, err := data.SucceedRun(context.Background(), liveMessage.RunID, 1, liveWorker); err != nil || !ok {
		t.Fatalf("live run completion = %v, %v", ok, err)
	}
	assertCounter(t, "zombies_recovered_total", "", zombies+5)
	assertCounter(t, "retries_total", "", retries+3)
	assertCounter(t, "executions_total", "failed", failed+3)
	assertCounter(t, "executions_total", "dead", dead+1)
	assertCounter(t, "executions_total", "cancelled", cancelled+1)
}

func TestStuckQueuedSweepPreservesAttemptAndStartGate(t *testing.T) {
	message, cancelled := queuedDispatch(t, store.Job{}), queuedDispatch(t, store.Job{})
	if _, err := data.CancelRun(context.Background(), cancelled.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`UPDATE job_runs SET queued_at=now()-interval '10 minutes' WHERE id::text=$1`, message.RunID); err != nil {
		t.Fatal(err)
	}
	result, err := data.Sweep(context.Background(), 100, 5*time.Minute)
	if err != nil || result.Requeued != 1 {
		t.Fatalf("queued sweep = %+v, %v", result, err)
	}
	run := waitForRun(t, message.RunID, "PENDING")
	if run.Attempt != 1 || run.QueuedAt != nil || !run.FireTime.Equal(message.FireTime) {
		t.Fatalf("queued recovery changed attempt/fire: %+v", run)
	}
	worker := registeredWorker(t, time.Minute)
	if claimed, err := data.StartRuns(context.Background(), []store.DispatchMessage{message}, worker); err != nil || len(claimed) != 0 {
		t.Fatalf("old delivery started before redispatch: %+v, %v", claimed, err)
	}
	queueRetry(t, message.RunID)
	claimed, err := data.StartRuns(context.Background(), []store.DispatchMessage{message, message, cancelled}, worker)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("redispatched duplicate/cancelled starts = %+v, %v", claimed, err)
	}
	if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, worker); err != nil || !ok {
		t.Fatalf("complete redispatched run = %v, %v", ok, err)
	}
	var attempts int
	if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text=$1`, message.RunID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempt history count = %d, %v", attempts, err)
	}
	waitForRun(t, cancelled.RunID, "CANCELLED")
}

func fastWorkerOptions(concurrency int) executorlib.Options {
	o := executorlib.DefaultOptions(concurrency)
	o.HeartbeatInterval, o.LeaseDuration, o.SafetyMargin = 100*time.Millisecond, 3*time.Second, 300*time.Millisecond
	o.DrainTimeout = 500 * time.Millisecond
	return o
}

func fastWatcherOptions() watcher.Options {
	o := watcher.DefaultOptions()
	o.DispatchInterval, o.SweepInterval = 20*time.Millisecond, 20*time.Millisecond
	o.QueuedTimeout, o.OperationTimeout = time.Minute, 2*time.Second
	return o
}

func startWatcher(t *testing.T, writer, dlq *kafka.Writer, options watcher.Options) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- watcher.Run(ctx, data, writer, dlq, logger, options) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("watcher stopped: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("watcher did not stop")
		}
	})
}

func TestWorkerHeartbeatsWhilePoolIsFull(t *testing.T) {
	durations := metric(t, "execution_duration_seconds", "http").GetHistogram().GetSampleCount()
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
	options := fastWorkerOptions(1)
	startConsumerWithOptions(t, topic, "full-pool-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, options)
	publishDispatch(t, writer, message)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("HTTP execution did not start")
	}
	run := waitForRun(t, message.RunID, "RUNNING")
	if got := metric(t, "inflight_jobs", "").GetGauge().GetValue(); got != 1 {
		t.Fatalf("inflight jobs = %v, want 1 while handler is blocked", got)
	}
	var before time.Time
	if err := database.QueryRow(`SELECT last_heartbeat FROM workers WHERE id::text=$1`, run.WorkerID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	time.Sleep(options.LeaseDuration + 100*time.Millisecond)
	stillRunning := waitForRun(t, message.RunID, "RUNNING")
	var after time.Time
	var live bool
	if err := database.QueryRow(`SELECT last_heartbeat, lease_expires_at>clock_timestamp() FROM workers WHERE id::text=$1`, run.WorkerID).Scan(&after, &live); err != nil {
		t.Fatal(err)
	}
	if stillRunning.WorkerID != run.WorkerID || !after.After(before) || !live {
		t.Fatal("a saturated pool prevented lease renewal")
	}
	releaseJob()
	waitForRun(t, message.RunID, "SUCCEEDED")
	if got := metric(t, "inflight_jobs", "").GetGauge().GetValue(); got != 0 {
		t.Fatalf("inflight jobs = %v, want 0 after execution (even while fetching)", got)
	}
	if got := metric(t, "execution_duration_seconds", "http").GetHistogram().GetSampleCount(); got != durations+1 {
		t.Fatalf("execution samples = %d, want %d", got, durations+1)
	}
}

func TestWorkerSelfFencesDuringBlockedHeartbeatAndResumes(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(cancelled)
	}))
	t.Cleanup(server.Close)
	payload, _ := json.Marshal(map[string]string{"url": server.URL})
	message := queuedDispatch(t, store.Job{Type: "http", Payload: payload, BackoffBaseMS: 10, BackoffMaxMS: 20})
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	options := fastWorkerOptions(2)
	startConsumerWithOptions(t, topic, "blocked-heartbeat-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, options)
	publishDispatch(t, writer, message)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("HTTP execution did not start")
	}
	oldRun := waitForRun(t, message.RunID, "RUNNING")
	lock, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Rollback() })
	if _, err := lock.Exec(`LOCK TABLE workers IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	blockedAt := time.Now()
	next := queuedDispatch(t, store.Job{})
	publishDispatch(t, writer, next)
	// Prove the next dispatch was fetched and its claim is waiting on workers.
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		var blocked bool
		if err := database.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE wait_event_type='Lock' AND query LIKE 'SELECT EXISTS (%'
			AND pid<>pg_backend_pid())`).Scan(&blocked); err != nil {
			return err
		}
		if !blocked {
			return errors.New("claim has not reached the blocked lease check")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(options.LeaseDuration + time.Second):
		t.Fatal("DB blockage prevented local self-fencing")
	}
	if time.Since(blockedAt) > options.LeaseDuration+time.Second {
		t.Fatal("local cancellation exceeded the lease deadline")
	}
	waitForRun(t, next.RunID, "QUEUED")
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	newRun := waitForRun(t, next.RunID, "SUCCEEDED")
	if newRun.WorkerID == oldRun.WorkerID || newRun.WorkerID == "" {
		t.Fatal("executor reused its fenced worker identity")
	}
	expireWorker(t, oldRun.WorkerID)
	if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, oldRun.WorkerID); err != nil || ok {
		t.Fatalf("old completion applied = %v, %v", ok, err)
	}
	if run, err := data.FailRun(context.Background(), message.RunID, 1, oldRun.WorkerID, store.Failure{Error: "late"}); err != nil || run.ID != "" {
		t.Fatalf("old failure applied = %+v, %v", run, err)
	}
	if _, err := data.Sweep(context.Background(), 100, time.Minute); err != nil {
		t.Fatal(err)
	}
	waitForRun(t, message.RunID, "PENDING")
}

func TestWatcherRecoversCrashedWorkerToAnotherExecutor(t *testing.T) {
	message := queuedDispatch(t, store.Job{MaxAttempts: 3, BackoffBaseMS: 10, BackoffMaxMS: 20})
	crashed := registeredWorker(t, 300*time.Millisecond)
	startAttempt(t, message, crashed)
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startConsumerWithOptions(t, topic, "crash-recovery-"+uuid.NewString(), executorlib.HTTPClient(nil), dlq, fastWorkerOptions(2))
	startWatcher(t, writer, dlq, fastWatcherOptions())
	run := waitForRun(t, message.RunID, "SUCCEEDED")
	if run.Attempt != 2 || run.WorkerID == crashed || !run.FireTime.Equal(message.FireTime) {
		t.Fatalf("crash recovery = %+v", run)
	}
	assertAttempt(t, message.RunID, 1, "FAILED", "worker lease expired", nil, 0)
	assertAttempt(t, message.RunID, 2, "SUCCEEDED", "", nil, 0)
	if ok, err := data.SucceedRun(context.Background(), message.RunID, 1, crashed); err != nil || ok {
		t.Fatalf("crashed worker's late result = %v, %v", ok, err)
	}
}

func TestWatcherRecoveredDeadRunGoesToDLQAndCancelWins(t *testing.T) {
	dead, cancelled := queuedDispatch(t, store.Job{MaxAttempts: 1}), queuedDispatch(t, store.Job{MaxAttempts: 1})
	crashed := registeredWorker(t, time.Minute)
	startAttempt(t, dead, crashed)
	startAttempt(t, cancelled, crashed)
	if _, err := data.CancelRun(context.Background(), cancelled.RunID); err != nil {
		t.Fatal(err)
	}
	expireWorker(t, crashed)
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startWatcher(t, writer, dlq, fastWatcherOptions())
	waitForRun(t, dead.RunID, "DEAD")
	waitForRun(t, cancelled.RunID, "CANCELLED")
	letter := readDeadLetter(t, dlqTopic)
	if letter.Message == nil || letter.Message.RunID != dead.RunID || letter.Message.Attempt != 1 || letter.Error != "worker lease expired" {
		t.Fatalf("recovered DEAD letter = %+v", letter)
	}
	var count int64
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for partition := range 3 {
		conn, err := kafka.DialLeader(ctx, "tcp", kafkaBrokers[0], dlqTopic, partition)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		last, err := conn.ReadLastOffset()
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		count += last
	}
	if count != 1 {
		t.Fatalf("DLQ records = %d; want one DEAD notification and no cancellation notification", count)
	}
}

func TestWatcherRedispatchesStuckQueuedWithoutDuplicateExecution(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	payload, _ := json.Marshal(map[string]string{"url": server.URL})
	message := queuedDispatch(t, store.Job{Type: "http", Payload: payload})
	if _, err := database.Exec(`UPDATE job_runs SET queued_at=now()-interval '10 minutes' WHERE id::text=$1`, message.RunID); err != nil {
		t.Fatal(err)
	}
	topic, dlqTopic := createKafkaTopic(t), createKafkaTopic(t)
	writer, dlq := kafkaWriter(topic), kafkaWriter(dlqTopic)
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	startWatcher(t, writer, dlq, fastWatcherOptions())
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := waitFor(ctx, func() error {
		run, err := data.GetRun(ctx, message.RunID)
		if err != nil {
			return err
		}
		if run.Status != "QUEUED" || run.QueuedAt == nil || time.Since(*run.QueuedAt) > time.Minute {
			return errors.New("watcher has not redispatched the overdue run")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Deliver the original copy again, then a marker in the same partition.
	// With capacity one, completing the marker proves both copies were handled.
	publishDispatch(t, writer, message)
	marker := queuedDispatch(t, store.Job{})
	value, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(message.RunID), Value: value}); err != nil {
		t.Fatal(err)
	}
	startConsumerWithOptions(t, topic, "stuck-queued-"+uuid.NewString(), executorlib.HTTPClient([]string{"127.0.0.1"}), dlq, fastWorkerOptions(1))
	run := waitForRun(t, message.RunID, "SUCCEEDED")
	waitForRun(t, marker.RunID, "SUCCEEDED")
	if run.Attempt != 1 || !run.FireTime.Equal(message.FireTime) || requests.Load() != 1 {
		t.Fatalf("queued redispatch = %+v, executions=%d", run, requests.Load())
	}
	var attempts int
	if err := database.QueryRow(`SELECT count(*) FROM run_attempts WHERE run_id::text=$1`, message.RunID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("redispatch attempt history count = %d, %v", attempts, err)
	}
}

func TestWatcherSweepsWhileKafkaPublishIsBlocked(t *testing.T) {
	zombie := queuedDispatch(t, store.Job{BackoffBaseMS: 10, BackoffMaxMS: 20})
	crashed := registeredWorker(t, time.Minute)
	startAttempt(t, zombie, crashed)
	pending, err := data.RunNow(context.Background(), zombie.JobID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = data.CancelRun(context.Background(), pending.ID) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	writer := &kafka.Writer{Addr: kafka.TCP(listener.Addr().String()), Topic: "jobs.run", RequiredAcks: kafka.RequireAll}
	dlq := kafkaWriter(createKafkaTopic(t))
	t.Cleanup(func() { _ = writer.Close(); _ = dlq.Close() })
	options := fastWatcherOptions()
	options.RetentionInterval = 20 * time.Millisecond
	startWatcher(t, writer, dlq, options)
	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher did not reach the blocked Kafka broker")
	}
	old := databaseNow(t).Add(-8 * 24 * time.Hour)
	expired := retentionRun(t, zombie.JobID, "SUCCEEDED", &old)
	expireWorker(t, crashed)
	ctx, stop := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stop()
	if err := waitFor(ctx, func() error {
		run, err := data.GetRun(ctx, zombie.RunID)
		if err != nil {
			return err
		}
		if run.Status != "PENDING" || run.Attempt != 2 {
			return fmt.Errorf("recovery was blocked by Kafka: %+v", run)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cron := dueCronJob(t, "fire_once", databaseNow(t).Add(-time.Minute))
	cronCtx, stopCron := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stopCron()
	if err := waitFor(cronCtx, func() error {
		var exists bool
		if err := database.QueryRowContext(cronCtx, `SELECT EXISTS (
			SELECT 1 FROM job_runs WHERE job_id::text=$1 AND fire_time=$2 AND status='PENDING'
		)`, cron.ID, cron.NextFireAt).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("cron materialization was blocked by Kafka")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	retentionCtx, stopRetention := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stopRetention()
	if err := waitFor(retentionCtx, func() error {
		_, err := data.GetRun(retentionCtx, expired)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("retention was blocked by Kafka")
	}); err != nil {
		t.Fatal(err)
	}
}
