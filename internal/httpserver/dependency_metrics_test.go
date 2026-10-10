package httpserver

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/zishan044/distributed-job-scheduler/internal/metrics"
)

func TestDependencyMetricsFailures(t *testing.T) {
	observations := metrics.New()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	failed := false
	observations.RegisterCluster(logger, func(ctx context.Context) (int64, error) {
		if failed {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return 2, nil
	}, func(context.Context) (int64, error) {
		if failed {
			return 0, errors.New("broker unavailable")
		}
		return 8, nil
	})
	router := NewAPIRouter(logger, nil, nil, "key", observations.Registry)
	// Verify healthy -> failed -> recovered without exposing stale or zero values.
	for _, fail := range []bool{false, true, false} {
		failed = fail
		response := httptest.NewRecorder()
		started := time.Now()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if response.Code != http.StatusOK || time.Since(started) > 4*time.Second {
			t.Fatalf("scrape status=%d, duration=%v", response.Code, time.Since(started))
		}
		parser := expfmt.NewTextParser(model.LegacyValidation)
		families, err := parser.TextToMetricFamilies(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]float64{"active_workers": 2, "kafka_consumer_lag": 8} {
			family := families[name]
			if family == nil || len(family.GetMetric()) != 1 {
				t.Fatalf("missing cluster metric %s", name)
			}
			got := family.GetMetric()[0].GetGauge().GetValue()
			if (fail && !math.IsNaN(got)) || (!fail && got != want) {
				t.Fatalf("%s = %v, failed=%v", name, got, fail)
			}
		}
		if families["jobs_created_total"] == nil {
			t.Fatal("dependency failure hid application metrics")
		}
	}
	for _, message := range []string{"context deadline exceeded", "broker unavailable"} {
		if !strings.Contains(logs.String(), message) {
			t.Fatalf("missing collection error log: %s", message)
		}
	}
}
