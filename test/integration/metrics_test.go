//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func metric(t *testing.T, name, label string) *dto.Metric {
	t.Helper()
	families, err := data.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, sample := range family.GetMetric() {
			if label == "" || (len(sample.GetLabel()) == 1 && sample.GetLabel()[0].GetValue() == label) {
				return sample
			}
		}
	}
	t.Fatalf("missing metric %s{%s}", name, label)
	return nil
}

func counterValue(t *testing.T, name, label string) float64 {
	t.Helper()
	return metric(t, name, label).GetCounter().GetValue()
}

func assertCounter(t *testing.T, name, label string, want float64) {
	t.Helper()
	// A committed row can become visible before the following metric update.
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	var got float64
	if err := waitFor(ctx, func() error {
		got = counterValue(t, name, label)
		if got != want {
			return fmt.Errorf("counter not updated")
		}
		return nil
	}); err != nil {
		t.Fatalf("%s{%s} = %v, want %v", name, label, got, want)
	}
}

func TestSweepMetricsWaitForCommit(t *testing.T) {
	worker := registeredWorker(t, time.Minute)
	first := queuedDispatch(t, store.Job{MaxAttempts: 3})
	second := queuedDispatch(t, store.Job{MaxAttempts: 1})
	startAttempt(t, first, worker)
	startAttempt(t, second, worker)
	expireWorker(t, worker)
	if _, err := database.Exec(`DELETE FROM run_attempts WHERE run_id::text=$1`, second.RunID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = database.Exec(`INSERT INTO run_attempts (run_id, attempt, worker_id, started_at)
			SELECT id, attempt, worker_id, started_at FROM job_runs WHERE id::text=$1
			ON CONFLICT DO NOTHING`, second.RunID)
	}()
	before := map[string]float64{
		"retries_total":           counterValue(t, "retries_total", ""),
		"zombies_recovered_total": counterValue(t, "zombies_recovered_total", ""),
		"failed":                  counterValue(t, "executions_total", "failed"),
		"dead":                    counterValue(t, "executions_total", "dead"),
		"cancelled":               counterValue(t, "executions_total", "cancelled"),
	}
	if _, err := data.Sweep(context.Background(), 2000, 5*time.Minute); err == nil {
		t.Fatal("sweep without attempt history should roll back")
	}
	waitForRun(t, first.RunID, "RUNNING")
	waitForRun(t, second.RunID, "RUNNING")
	for name, want := range before {
		metricName, label := name, ""
		if name == "failed" || name == "dead" || name == "cancelled" {
			metricName, label = "executions_total", name
		}
		assertCounter(t, metricName, label, want)
	}
}
