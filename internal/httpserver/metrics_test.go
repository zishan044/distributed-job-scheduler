package httpserver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/zishan044/distributed-job-scheduler/internal/metrics"
)

func TestMetricsEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, service := range []string{"api", "watcher", "executor"} {
		t.Run(service, func(t *testing.T) {
			// Creating two registries must neither panic nor share observations.
			observations, other := metrics.New(), metrics.New()
			observations.JobsCreated.Inc()
			observations.StartLatency.Observe(1.5)
			for index, instance := range []*metrics.Metrics{observations, other} {
				var router http.Handler
				if service == "api" {
					router = NewAPIRouter(logger, nil, nil, "test-key", instance.Registry)
				} else {
					router = NewRouter(service, logger, instance.Registry)
				}
				response := httptest.NewRecorder()
				// Prometheus can scrape without the API's Bearer token.
				router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain") {
					t.Fatalf("scrape = %d, %q", response.Code, response.Header().Get("Content-Type"))
				}
				parser := expfmt.NewTextParser(model.LegacyValidation)
				families, err := parser.TextToMetricFamilies(response.Body)
				if err != nil {
					t.Fatalf("invalid Prometheus exposition: %v", err)
				}
				for name, kind := range map[string]string{
					"jobs_created_total":         "COUNTER",
					"watcher_claimed_total":      "COUNTER",
					"executions_total":           "COUNTER",
					"dispatch_latency_seconds":   "HISTOGRAM",
					"start_latency_seconds":      "HISTOGRAM",
					"execution_duration_seconds": "HISTOGRAM",
					"inflight_jobs":              "GAUGE",
					"retries_total":              "COUNTER",
					"dlq_total":                  "COUNTER",
					"zombies_recovered_total":    "COUNTER",
					"stale_completions_total":    "COUNTER",
					"redis_errors_total":         "COUNTER",
				} {
					family := families[name]
					if family == nil || family.GetType().String() != kind {
						t.Fatalf("%s missing or has wrong metric type", name)
					}
					for _, sample := range family.GetMetric() {
						for _, label := range sample.GetLabel() {
							if label.GetName() != "type" && label.GetName() != "result" {
								t.Fatalf("unexpected application label: %s", label.GetName())
							}
						}
					}
				}
				if len(families["executions_total"].GetMetric()) != 4 || len(families["execution_duration_seconds"].GetMetric()) != 2 {
					t.Fatal("fixed label values must be exported before the first execution")
				}
				if families["active_workers"] != nil || families["kafka_consumer_lag"] != nil {
					t.Fatal("uncollected cluster gauges must not report zero")
				}
				want := float64(1 - index)
				if got := families["jobs_created_total"].GetMetric()[0].GetCounter().GetValue(); got != want {
					t.Fatalf("registry %d created count = %v, want %v", index, got, want)
				}
				histogram := families["start_latency_seconds"].GetMetric()[0].GetHistogram()
				foundSLA := false
				for _, bucket := range histogram.GetBucket() {
					if bucket.GetUpperBound() == 2 {
						foundSLA = true
						if bucket.GetCumulativeCount() != uint64(1-index) {
							t.Fatal("SLA bucket observation leaked across registries")
						}
					}
				}
				if !foundSLA {
					t.Fatal("start latency histogram needs a 2s SLA bucket")
				}
				response = httptest.NewRecorder()
				router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
				if response.Code != http.StatusOK {
					t.Fatalf("healthz = %d", response.Code)
				}
				if service == "api" {
					response = httptest.NewRecorder()
					router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/jobs/test", nil))
					if response.Code != http.StatusUnauthorized {
						t.Fatalf("API auth = %d, want 401", response.Code)
					}
				}
			}
		})
	}
}
