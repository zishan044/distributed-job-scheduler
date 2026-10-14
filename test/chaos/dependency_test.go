//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
	"github.com/zishan044/distributed-job-scheduler/pkg/client"
)

func getURL(ctx context.Context, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d", url, response.StatusCode)
	}
	return io.ReadAll(io.LimitReader(response.Body, 2<<20))
}

func (h *harness) waitService(ctx context.Context, item container) error {
	service := item.Config.Labels["com.docker.compose.service"]
	return waitFor(ctx, func(ctx context.Context) error {
		current, err := inspect(ctx, item.ID, service)
		if err != nil {
			return err
		}
		if !current.State.Running || current.State.Paused {
			return fmt.Errorf("%s is not running", service)
		}
		if service == "postgres" || service == "redis" || service == "kafka" {
			if current.State.Health.Status != "healthy" {
				return fmt.Errorf("%s health = %s", service, current.State.Health.Status)
			}
			return nil
		}
		ip := current.NetworkSettings.Networks[projectName+"_default"].IPAddress
		if net.ParseIP(ip) == nil {
			return fmt.Errorf("missing network address for %s", service)
		}
		_, err = getURL(ctx, "http://"+net.JoinHostPort(ip, "8080")+"/healthz")
		return err
	})
}

func (h *harness) dependency(ctx context.Context, service string) container {
	h.t.Helper()
	items, err := containers(ctx, service)
	if err != nil || len(items) != 1 {
		h.t.Fatalf("%s discovery: %v, %v", service, items, err)
	}
	return items[0]
}

func (h *harness) restoreDependency(item container) func() {
	restore := sync.OnceFunc(func() {
		ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		service := item.Config.Labels["com.docker.compose.service"]
		current, err := inspect(ctx, item.ID, service)
		if err == nil && !current.State.Running {
			_, err = docker(ctx, "start", item.ID)
		}
		if err == nil {
			err = h.waitService(ctx, item)
		}
		if err != nil {
			h.t.Errorf("restore %s: %v", service, err)
		}
	})
	h.t.Cleanup(restore)
	return restore
}

type loadBatch struct {
	Key      string                 `json:"key"`
	Requests []api.CreateJobRequest `json:"requests"`
	Jobs     []api.Job              `json:"jobs,omitempty"`
	Error    string                 `json:"error,omitempty"`
	Replayed bool                   `json:"replayed"`
}

func TestKafkaRestartUnderLoad(t *testing.T)    { dependencyRestart(t, "kafka") }
func TestPostgresRestartUnderLoad(t *testing.T) { dependencyRestart(t, "postgres") }

func dependencyRestart(t *testing.T, service string) {
	ctx, cancel := context.WithTimeout(suiteCtx, 8*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	item := h.dependency(ctx, service)
	restore := h.restoreDependency(item)
	start := time.Now()
	batches := make([]loadBatch, 30)
	for i := range batches {
		when := start.Add(time.Duration(i) * time.Second).UTC()
		batches[i].Key = uuid.NewString()
		for j := range 100 {
			name := fmt.Sprintf("%s%s/%02d/%03d", t.Name(), h.path, i, j)
			h.cohort = append(h.cohort, name)
			batches[i].Requests = append(batches[i].Requests, api.CreateJobRequest{
				Name: name, Type: "handler", Payload: []byte(`{"handler":"sleep","duration_ms":500}`),
				ScheduledFor: &when, MaxAttempts: 5, BackoffBaseMS: 500, BackoffMaxMS: 500, TimeoutMS: 30000,
			})
		}
	}
	t.Cleanup(func() { h.write("submissions.json", batches) })
	faultDone := make(chan error, 1)
	var faultStarted bool
	for i := range batches {
		timer := time.NewTimer(time.Until(start.Add(time.Duration(i) * time.Second)))
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
		requestCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		jobs, err := h.client.ScheduleBatch(requestCtx, batches[i].Requests, client.WithIdempotencyKey(batches[i].Key))
		stop()
		if err != nil {
			batches[i].Error = err.Error()
		} else {
			batches[i].Jobs = jobs
		}
		if i == 4 {
			counts, err := h.data.ChaosCohortCounts(ctx, h.cohort[:500])
			if err != nil || counts.Jobs != 500 || counts.Succeeded >= counts.Runs {
				t.Fatalf("no accepted outstanding work at restart: %+v, %v", counts, err)
			}
			h.event("restart_during_load", map[string]any{"service": service, "counts": counts, "id": item.ID})
			faultStarted = true
			go func() { _, err := docker(ctx, "restart", "--time=1", item.ID); faultDone <- err }()
		}
	}
	window := time.NewTimer(time.Until(start.Add(30 * time.Second)))
	select {
	case <-ctx.Done():
		window.Stop()
		t.Fatal(ctx.Err())
	case <-window.C:
	}
	if !faultStarted {
		t.Fatal("fault was not injected")
	}
	if err := <-faultDone; err != nil {
		t.Fatal(err)
	}
	restore()
	h.event("offered_3000_jobs", map[string]any{"elapsed_seconds": time.Since(start).Seconds()})
	seen := make(map[string]bool)
	for i := range batches {
		if batches[i].Error != "" {
			if service != "postgres" {
				t.Fatalf("unexpected ingest error during Kafka fault: %s", batches[i].Error)
			}
			if err := waitFor(ctx, func(ctx context.Context) error {
				jobs, err := h.client.ScheduleBatch(ctx, batches[i].Requests, client.WithIdempotencyKey(batches[i].Key))
				if err == nil {
					batches[i].Jobs = jobs
					batches[i].Replayed = true
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		if len(batches[i].Jobs) != 100 {
			t.Fatalf("batch %d returned %d jobs", i, len(batches[i].Jobs))
		}
		for j, job := range batches[i].Jobs {
			if _, err := uuid.Parse(job.ID); err != nil || seen[job.ID] || job.Name != batches[i].Requests[j].Name {
				t.Fatalf("batch response changed identity/order: %+v", job)
			}
			seen[job.ID] = true
		}
	}
	drainCtx, stop := context.WithTimeout(ctx, 6*time.Minute)
	defer stop()
	var lastProgress time.Time
	if err := waitFor(drainCtx, func(ctx context.Context) error {
		counts, err := h.data.ChaosCohortCounts(ctx, h.cohort)
		if time.Since(lastProgress) > 10*time.Second {
			h.event("drain_progress", counts)
			lastProgress = time.Now()
		}
		if err != nil {
			return err
		}
		if counts.Jobs != 3000 || counts.Names != 3000 || counts.Runs != 3000 || !counts.OneRunEach || counts.Succeeded != 3000 {
			return fmt.Errorf("unfinished or mismatched accepted cohort: %+v", counts)
		}
		h.event("verified_dependency_recovery", map[string]any{"service": service, "counts": counts})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(ctx, func(ctx context.Context) error {
		count, err := h.data.ActiveWorkers(ctx)
		if err != nil || count != 3 {
			return fmt.Errorf("recovered worker count = %d: %v", count, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
