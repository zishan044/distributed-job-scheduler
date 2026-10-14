//go:build chaos

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

func (h *harness) scheduleHTTP(ctx context.Context) (api.Job, api.Run) {
	h.t.Helper()
	now := time.Now().UTC()
	job, err := h.client.ScheduleHTTP(ctx, api.CreateJobRequest{
		Name: "chaos-" + h.t.Name() + h.path, ScheduledFor: &now,
		MaxAttempts: 3, BackoffBaseMS: 500, BackoffMaxMS: 500, TimeoutMS: 120000,
	}, api.HTTPPayload{URL: "http://chaos-runner:8090" + h.path, Method: "POST"})
	if err != nil {
		h.t.Fatal(err)
	}
	runs, err := h.client.ListRuns(ctx, job.ID, 100, 0)
	if err != nil || len(runs) != 1 {
		h.t.Fatalf("initial runs = %v, %v", runs, err)
	}
	h.runID = runs[0].ID
	h.event("scheduled", runs[0])
	return job, runs[0]
}

func (h *harness) waitRun(ctx context.Context, status string) api.Run {
	h.t.Helper()
	var run api.Run
	if err := waitFor(ctx, func(ctx context.Context) error {
		var err error
		run, err = h.client.Get(ctx, h.runID)
		if err != nil || run.Status != status {
			return fmt.Errorf("waiting for %s: %+v, %v", status, run, err)
		}
		return nil
	}); err != nil {
		h.t.Fatal(err)
	}
	return run
}

func (h *harness) heldExecutor(ctx context.Context) (api.Run, container) {
	h.t.Helper()
	run := h.waitRun(ctx, api.StatusRunning)
	if err := waitFor(ctx, func(context.Context) error {
		calls := target.calls(h.path)
		if len(calls) != 1 || calls[0].Attempt != 1 || calls[0].IdempotencyKey != run.ID || calls[0].FinishedAt != nil {
			return fmt.Errorf("waiting for held attempt 1: %+v", calls)
		}
		return nil
	}); err != nil {
		h.t.Fatal(err)
	}
	workers, err := h.data.ChaosWorkers(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	for _, worker := range workers {
		if worker.ID == run.WorkerID && worker.Live {
			for _, item := range h.executors {
				if item.Config.Hostname == worker.Hostname {
					current, err := inspect(ctx, item.ID, "executor")
					if err != nil {
						h.t.Fatal(err)
					}
					h.event("identified_owner", map[string]any{"run": run, "worker": worker, "container": current})
					return run, current
				}
			}
		}
	}
	h.t.Fatalf("no live executor owns %+v", run)
	return api.Run{}, container{}
}

func (h *harness) killExecutor(ctx context.Context, item container) (time.Time, func()) {
	h.t.Helper()
	current, err := inspect(ctx, item.ID, "executor")
	if err != nil {
		h.t.Fatal(err)
	}
	policy := current.HostConfig.RestartPolicy.Name
	if policy == "on-failure" {
		policy += ":" + strconv.Itoa(current.HostConfig.RestartPolicy.MaximumRetryCount)
	}
	restore := sync.OnceFunc(func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		current, err := inspect(restoreCtx, item.ID, "executor")
		if err == nil {
			_, err = docker(restoreCtx, "update", "--restart="+policy, item.ID)
		}
		if err == nil && !current.State.Running {
			_, err = docker(restoreCtx, "start", item.ID)
		}
		if err == nil {
			err = waitFor(restoreCtx, func(ctx context.Context) error {
				count, err := h.data.ActiveWorkers(ctx)
				if err != nil || count != 3 {
					return fmt.Errorf("restored worker count = %d: %v", count, err)
				}
				return nil
			})
		}
		if err != nil {
			h.t.Errorf("restore killed executor: %v", err)
		} else {
			h.event("restored_executor", map[string]any{"id": item.ID, "restart_policy": policy})
		}
	})
	h.t.Cleanup(restore) // Includes an ambiguous update/kill response.
	if _, err := docker(ctx, "update", "--restart=no", item.ID); err != nil {
		h.t.Fatal(err)
	}
	killedAt := time.Now()
	if _, err := docker(ctx, "kill", "--signal=KILL", item.ID); err != nil {
		h.t.Fatal(err)
	}
	h.event("killed_executor", map[string]any{"id": item.ID, "at": killedAt.UTC(), "original_restart_policy": policy})
	return killedAt, restore
}

func (h *harness) assertRecovery(ctx context.Context, initial, owner api.Run) (api.Run, []store.ChaosAttempt) {
	h.t.Helper()
	run := h.waitRun(ctx, api.StatusSucceeded)
	runs, err := h.client.ListRuns(ctx, initial.JobID, 100, 0)
	if err != nil || len(runs) != 1 || runs[0].ID != initial.ID {
		h.t.Fatalf("recovery created another fire: %+v, %v", runs, err)
	}
	attempts, err := h.data.ChaosAttempts(ctx, run.ID)
	if err != nil || len(attempts) != 2 {
		h.t.Fatalf("recovery attempts = %+v, %v", attempts, err)
	}
	if run.Attempt != 2 || run.WorkerID == owner.WorkerID || run.ID != initial.ID ||
		run.JobID != initial.JobID || !run.FireTime.Equal(initial.FireTime) ||
		attempts[0].Attempt != 1 || attempts[0].WorkerID != owner.WorkerID ||
		attempts[0].Outcome != api.StatusFailed || attempts[0].Error != "worker lease expired" ||
		attempts[0].FinishedAt == nil || attempts[1].Attempt != 2 || attempts[1].WorkerID != run.WorkerID ||
		attempts[1].Outcome != api.StatusSucceeded || attempts[1].FinishedAt == nil {
		h.t.Fatalf("unexpected recovery: run=%+v; attempts=%+v", run, attempts)
	}
	calls := target.calls(h.path)
	if len(calls) != 2 || calls[0].Attempt != 1 || calls[1].Attempt != 2 ||
		calls[0].IdempotencyKey != run.ID || calls[1].IdempotencyKey != run.ID ||
		calls[1].FinishedAt == nil || calls[1].Cancelled {
		h.t.Fatalf("HTTP recovery calls = %+v", calls)
	}
	h.event("recovered", map[string]any{"run": run, "attempts": attempts, "http": calls})
	return run, attempts
}

func TestExecutorKilledMidJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(suiteCtx, 2*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	release := target.hold(h.path, 1)
	t.Cleanup(release)
	_, initial := h.scheduleHTTP(ctx)
	owner, item := h.heldExecutor(ctx)
	killedAt, restore := h.killExecutor(ctx, item)
	recoveryCtx, stop := context.WithDeadline(ctx, killedAt.Add(20*time.Second))
	defer stop()
	run, _ := h.assertRecovery(recoveryCtx, initial, owner)
	if elapsed := time.Since(killedAt); elapsed > 20*time.Second {
		t.Fatalf("recovery took %s, exceeding 20 seconds", elapsed)
	}
	workers, err := h.data.ChaosWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recoveryHostname := ""
	for _, worker := range workers {
		if worker.ID == run.WorkerID {
			recoveryHostname = worker.Hostname
		}
	}
	if recoveryHostname == "" || recoveryHostname == item.Config.Hostname {
		t.Fatalf("recovered attempt's executor hostname = %q; killed hostname = %q", recoveryHostname, item.Config.Hostname)
	}
	h.event("verified_kill", map[string]any{"recovery_seconds": time.Since(killedAt).Seconds(), "run": run})
	release()
	restore()
}

func TestExecutorFrozenZombie(t *testing.T) {
	ctx, cancel := context.WithTimeout(suiteCtx, 2*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	release := target.hold(h.path, 1)
	t.Cleanup(release)
	_, initial := h.scheduleHTTP(ctx)
	owner, item := h.heldExecutor(ctx)
	h.pause(ctx, item)
	pausedAt := time.Now()
	recoveryCtx, stop := context.WithDeadline(ctx, pausedAt.Add(25*time.Second))
	defer stop()
	winner, history := h.assertRecovery(recoveryCtx, initial, owner)
	current, err := inspect(ctx, item.ID, "executor")
	if err != nil || !current.State.Paused {
		t.Fatalf("owner was not paused when recovery finished: %+v, %v", current, err)
	}
	timer := time.NewTimer(time.Until(pausedAt.Add(25 * time.Second)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	h.event("completed_25_second_freeze", map[string]any{"id": item.ID, "freeze_seconds": time.Since(pausedAt).Seconds()})
	h.resume(ctx, item)
	var logs []byte
	var renewed store.ChaosWorker
	if err := waitFor(ctx, func(ctx context.Context) error {
		if _, err := inspect(ctx, item.ID, "executor"); err != nil {
			return err
		}
		logs, err = docker(ctx, "logs", "--since", pausedAt.UTC().Format(time.RFC3339Nano), item.ID)
		if err != nil {
			return err
		}
		fenced := false
		for _, line := range strings.Split(string(logs), "\n") {
			var record struct {
				Message  string `json:"msg"`
				WorkerID string `json:"worker_id"`
			}
			if json.Unmarshal([]byte(line), &record) == nil && record.Message == "worker self-fenced" && record.WorkerID == owner.WorkerID {
				fenced = true
			}
		}
		if !fenced {
			return fmt.Errorf("waiting for old worker self-fencing: %s", logs)
		}
		workers, err := h.data.ChaosWorkers(ctx)
		if err != nil {
			return err
		}
		for _, worker := range workers {
			if worker.Hostname == item.Config.Hostname && worker.ID != owner.WorkerID && worker.Live {
				renewed = worker
				return nil
			}
		}
		return fmt.Errorf("waiting for a fresh worker identity on %s", item.Config.Hostname)
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.directory, "self-fence.log"), logs, 0644); err != nil {
		t.Fatal(err)
	}
	release()
	if err := waitFor(ctx, func(context.Context) error {
		calls := target.calls(h.path)
		if len(calls) != 2 || calls[0].FinishedAt == nil {
			return fmt.Errorf("waiting for old HTTP call to exit: %+v", calls)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	after := h.waitRun(ctx, api.StatusSucceeded)
	afterHistory, err := h.data.ChaosAttempts(ctx, h.runID)
	if err != nil || !reflect.DeepEqual(winner, after) || !reflect.DeepEqual(history, afterHistory) {
		t.Fatalf("resumed zombie changed winner/history: %+v, %+v, %v", after, afterHistory, err)
	}
	h.event("verified_freeze", map[string]any{"winner": after, "attempts": afterHistory, "renewed_worker": renewed, "http": target.calls(h.path)})
}
