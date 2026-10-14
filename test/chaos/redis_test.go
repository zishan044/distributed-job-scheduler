//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
	"github.com/zishan044/distributed-job-scheduler/pkg/client"
)

func redisCounter(ctx context.Context) (float64, error) {
	body, err := getURL(ctx, "http://api:8080/metrics")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "redis_errors_total" {
			return strconv.ParseFloat(fields[1], 64)
		}
	}
	return 0, fmt.Errorf("missing redis_errors_total")
}

func TestRedisStopped(t *testing.T) {
	ctx, cancel := context.WithTimeout(suiteCtx, 2*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	before, err := redisCounter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(time.Minute).UTC()
	request := api.CreateJobRequest{Name: "redis-before" + h.path, Type: "handler", Payload: []byte(`{"handler":"noop"}`), ScheduledFor: &when}
	key := uuid.NewString()
	original, err := h.client.Schedule(ctx, request, client.WithIdempotencyKey(key))
	if err != nil {
		t.Fatal(err)
	}
	h.cohort = append(h.cohort, request.Name)
	item := h.dependency(ctx, "redis")
	restore := h.restoreDependency(item)
	if _, err := inspect(ctx, item.ID, "redis"); err != nil {
		t.Fatal(err)
	}
	if _, err := docker(ctx, "stop", "--time=1", item.ID); err != nil {
		t.Fatal(err)
	}
	h.event("stopped_redis", item.ID)
	replayed, err := h.client.Schedule(ctx, request, client.WithIdempotencyKey(key))
	if err != nil || replayed.ID != original.ID {
		t.Fatalf("pre-outage replay = %+v, %v", replayed, err)
	}
	runs, err := h.client.ListRuns(ctx, original.ID, 100, 0)
	if err != nil || len(runs) != 1 {
		t.Fatalf("initial runs = %v, %v", runs, err)
	}
	h.runID = runs[0].ID
	if _, err := h.client.Cancel(ctx, h.runID); err != nil {
		t.Fatal(err)
	}
	h.waitRun(ctx, api.StatusCancelled)
	for _, stage := range []string{api.StatusPending, api.StatusQueued, api.StatusRunning} {
		if stage == api.StatusQueued {
			for _, executor := range h.executors {
				h.pause(ctx, executor)
			}
		}
		when := time.Now().UTC()
		if stage == api.StatusPending {
			when = when.Add(time.Minute)
		}
		request := api.CreateJobRequest{Name: "redis-" + stage + h.path, Type: "handler", Payload: []byte(`{"handler":"sleep","duration_ms":60000}`), ScheduledFor: &when, TimeoutMS: 120000}
		h.cohort = append(h.cohort, request.Name)
		key := uuid.NewString()
		created, err := h.client.Schedule(ctx, request, client.WithIdempotencyKey(key))
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := h.client.Schedule(ctx, request, client.WithIdempotencyKey(key))
		if err != nil || replayed.ID != created.ID {
			t.Fatalf("outage replay = %+v, %v", replayed, err)
		}
		runs, err := h.client.ListRuns(ctx, created.ID, 100, 0)
		if err != nil || len(runs) != 1 {
			t.Fatalf("outage initial runs = %v, %v", runs, err)
		}
		h.runID = runs[0].ID
		initial := h.waitRun(ctx, stage)
		if _, err := h.client.Cancel(ctx, h.runID); err != nil {
			t.Fatal(err)
		}
		cancelled := h.waitRun(ctx, api.StatusCancelled)
		attempts, err := h.data.ChaosAttempts(ctx, h.runID)
		if err != nil || cancelled.Attempt != 1 || !cancelled.FireTime.Equal(initial.FireTime) {
			t.Fatalf("cancel changed identity: %+v, %v", cancelled, err)
		}
		if stage == api.StatusRunning {
			if len(attempts) != 1 || attempts[0].Outcome != api.StatusCancelled {
				t.Fatalf("running cancellation = %+v", attempts)
			}
		} else if len(attempts) != 0 {
			t.Fatalf("cancelled waiting run executed: %+v", attempts)
		}
		h.event("verified_cancel_"+stage, map[string]any{"run": cancelled, "attempts": attempts, "job_id": created.ID})
		if stage == api.StatusQueued {
			for _, executor := range h.executors {
				h.resume(ctx, executor)
			}
		}
	}
	after, err := redisCounter(ctx)
	if err != nil || after <= before {
		t.Fatalf("Redis error counter = %f -> %f, %v", before, after, err)
	}
	counts, err := h.data.ChaosCohortCounts(ctx, h.cohort)
	if err != nil || counts.Jobs != 4 || counts.Names != 4 || counts.Runs != 4 || !counts.OneRunEach {
		t.Fatalf("Redis idempotency cohort = %+v, %v", counts, err)
	}
	h.event("verified_redis_fallback", map[string]any{"counts": counts, "errors_before": before, "errors_after": after})
	restore()
}
