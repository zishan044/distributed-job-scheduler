//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/queue"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

func TestWatcherKilledMidTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(suiteCtx, 2*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	for _, item := range h.executors {
		h.pause(ctx, item)
	}
	_, initial := h.scheduleHTTPAt(ctx, time.Now().Add(3*time.Second).UTC())
	barrier, err := h.data.ChaosHoldDispatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := barrier.Release(cleanupCtx); err != nil {
			t.Error(err)
		}
	})
	var blocked store.ChaosBlockedDispatch
	if err := waitFor(ctx, func(ctx context.Context) error {
		var err error
		blocked, err = barrier.Blocked(ctx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var victim container
	for _, watcher := range h.watchers {
		if watcher.NetworkSettings.Networks[projectName+"_default"].IPAddress == blocked.Address {
			victim = watcher
		}
	}
	if victim.ID == "" {
		t.Fatalf("blocked backend is not a watcher: %+v", blocked)
	}
	h.event("published_before_commit", map[string]any{"barrier_pid": barrier.PID, "blocked": blocked, "watcher": victim.ID})
	_, restore := h.killService(ctx, victim, "watcher")
	if err := waitFor(ctx, func(ctx context.Context) error {
		alive, err := h.data.ChaosBackendAlive(ctx, blocked.PID)
		if err != nil || alive {
			return fmt.Errorf("killed backend alive = %v: %v", alive, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	run := h.waitRun(ctx, api.StatusPending)
	attempts, err := h.data.ChaosAttempts(ctx, h.runID)
	if err != nil || run.Attempt != 1 || len(attempts) != 0 {
		t.Fatalf("rollback changed run: %+v, %+v, %v", run, attempts, err)
	}
	var survivor store.ChaosBlockedDispatch
	if err := waitFor(ctx, func(ctx context.Context) error {
		var err error
		survivor, err = barrier.Blocked(ctx)
		if err != nil {
			return err
		}
		if survivor.PID == blocked.PID || survivor.Address == blocked.Address {
			return fmt.Errorf("waiting for survivor: %+v", survivor)
		}
		for _, watcher := range h.watchers {
			if watcher.ID != victim.ID && watcher.NetworkSettings.Networks[projectName+"_default"].IPAddress == survivor.Address {
				_, err := inspect(ctx, watcher.ID, "watcher")
				return err
			}
		}
		return fmt.Errorf("survivor is not a validated watcher: %+v", survivor)
	}); err != nil {
		t.Fatal(err)
	}
	h.event("survivor_redispatched", map[string]any{"blocked": survivor, "rolled_back_run": run})
	if err := barrier.Release(ctx); err != nil {
		t.Fatal(err)
	}
	for _, item := range h.executors {
		h.resume(ctx, item)
	}
	run = h.waitRun(ctx, api.StatusSucceeded)
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	broker := &kafka.Client{Addr: kafka.TCP("kafka:9092"), Transport: transport}
	if err := waitFor(ctx, func(ctx context.Context) error {
		lag, err := queue.ConsumerLag(ctx, broker, "jobs.run", "executors")
		if err != nil || lag != 0 {
			return fmt.Errorf("remaining lag = %d: %v", lag, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	attempts, err = h.data.ChaosAttempts(ctx, h.runID)
	calls := target.calls(h.path)
	if err != nil || run.Attempt != 1 || !run.FireTime.Equal(initial.FireTime) || len(attempts) != 1 || attempts[0].Outcome != api.StatusSucceeded ||
		len(calls) != 1 || calls[0].IdempotencyKey != h.runID || calls[0].Attempt != 1 {
		t.Fatalf("watcher crash lost/duplicated execution: %+v, %+v, %+v, %v", run, attempts, calls, err)
	}
	h.event("verified_watcher_recovery", map[string]any{"run": run, "attempts": attempts, "http": calls, "kafka_lag": 0})
	restore()
}
