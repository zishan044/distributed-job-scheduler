// Command smoke generates a small workload and checks the Compose dashboard.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type smoke struct {
	api, prometheus, grafana string
	key, password            string
	client                   *http.Client
}

func main() {
	s := smoke{client: &http.Client{Timeout: 5 * time.Second}}
	flag.StringVar(&s.api, "api-url", "http://localhost:8080", "scheduler API URL")
	flag.StringVar(&s.prometheus, "prometheus-url", "http://localhost:9090", "Prometheus URL")
	flag.StringVar(&s.grafana, "grafana-url", "http://localhost:3000", "Grafana URL")
	jobs := flag.Int("jobs", 40, "number of ten-second sleep jobs (plus four outcome checks)")
	flag.Parse()
	s.key = os.Getenv("API_KEY")
	if s.key == "" {
		s.key = "scheduler-local"
	}
	s.password = os.Getenv("GRAFANA_ADMIN_PASSWORD")
	if s.password == "" {
		s.password = "scheduler-local"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := s.run(ctx, *jobs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (s *smoke) request(ctx context.Context, method, address string, body, result any) error {
	var content []byte
	if body != nil {
		var err error
		content, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(content))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if strings.HasPrefix(address, s.api+"/") {
		req.Header.Set("Authorization", "Bearer "+s.key)
	}
	if strings.HasPrefix(address, s.grafana+"/") {
		req.SetBasicAuth("admin", s.password)
	}
	response, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("%s %s: HTTP %d: %s", method, address, response.StatusCode, message)
	}
	if result == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		return err
	}
	return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(result)
}

func (s *smoke) query(ctx context.Context, endpoint, expression string) ([]float64, error) {
	var response struct {
		Status string
		Error  string
		Data   struct {
			Result []struct{ Value []json.RawMessage }
		}
	}
	if err := s.request(ctx, http.MethodGet, endpoint+"?query="+url.QueryEscape(expression), nil, &response); err != nil {
		return nil, err
	}
	if response.Status != "success" || len(response.Data.Result) == 0 {
		return nil, fmt.Errorf("query %s returned no data or failed: %s", expression, response.Error)
	}
	values := make([]float64, 0, len(response.Data.Result))
	for _, sample := range response.Data.Result {
		if len(sample.Value) != 2 {
			return nil, fmt.Errorf("invalid sample for %s", expression)
		}
		var raw string
		if err := json.Unmarshal(sample.Value[1], &raw); err != nil {
			return nil, err
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("query %s returned unknown value %q", expression, raw)
		}
		values = append(values, value)
	}
	return values, nil
}

func wait(ctx context.Context, check func() error) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		err := check()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("smoke timed out: %v: %w", err, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *smoke) run(ctx context.Context, count int) error {
	if count < 1 || count > 1000 {
		return fmt.Errorf("jobs must be between 1 and 1000")
	}
	queryURL := s.prometheus + "/api/v1/query"
	if err := wait(ctx, func() error {
		if err := s.request(ctx, http.MethodGet, s.api+"/healthz", nil, nil); err != nil {
			return err
		}
		values, err := s.query(ctx, queryURL, `min by (job) (up{job=~"api|watcher|executor"})`)
		if err != nil {
			return err
		}
		if len(values) != 3 {
			return fmt.Errorf("waiting for all three service scrape jobs")
		}
		for _, value := range values {
			if value < 1 {
				return fmt.Errorf("waiting for healthy scrape targets")
			}
		}
		// Counter rates need two samples before we generate activity.
		_, err = s.query(ctx, queryURL, `sum(rate(jobs_created_total{job="api"}[1m]))`)
		return err
	}); err != nil {
		return err
	}
	before := make(map[string]float64)
	for _, metric := range []string{"dlq_total", "retries_total"} {
		expression := `sum(` + metric + `{job=~"watcher|executor"})`
		values, err := s.query(ctx, queryURL, expression)
		if err != nil {
			return err
		}
		before[expression] = values[0]
	}
	type expectedJob struct {
		id, status string
		attempt    int
	}
	jobs := make([]expectedJob, 0, count+4)
	for i := 0; i < count+4; i++ {
		jobType, status, attempt := "handler", "SUCCEEDED", 1
		payload := map[string]any{"handler": "sleep", "duration_ms": 10000}
		switch i {
		case 0:
			payload = map[string]any{"handler": "noop"}
		case 1:
			jobType = "http"
			payload = map[string]any{"url": "http://echo:5678/"}
		case 2:
			payload = map[string]any{"handler": "fail", "permanent": true}
			status = "DEAD"
		case 3:
			payload = map[string]any{"handler": "fail"}
			status, attempt = "DEAD", 3
		}
		var created struct{ ID string }
		if err := s.request(ctx, http.MethodPost, s.api+"/v1/jobs", map[string]any{
			"name": fmt.Sprintf("m4-smoke-%d", i), "type": jobType, "payload": payload,
			"scheduled_for": time.Now().UTC(), "max_attempts": 3, "timeout_ms": 15000,
			"backoff_base_ms": 100, "backoff_max_ms": 1000,
		}, &created); err != nil {
			return err
		}
		jobs = append(jobs, expectedJob{created.ID, status, attempt})
	}
	fmt.Printf("Submitted %d jobs; watch in-flight jobs, lag, latency, retries and DLQ in Grafana.\n", len(jobs))
	if err := wait(ctx, func() error {
		for _, job := range jobs {
			var runs []struct {
				Status  string
				Attempt int
			}
			if err := s.request(ctx, http.MethodGet, s.api+"/v1/jobs/"+job.id+"/runs?limit=1", nil, &runs); err != nil {
				return err
			}
			if len(runs) != 1 || runs[0].Status != job.status || runs[0].Attempt != job.attempt {
				return fmt.Errorf("job %s waiting for %s attempt %d; got %v", job.id, job.status, job.attempt, runs)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	var dashboard struct {
		Meta      struct{ Provisioned bool }
		Dashboard struct {
			Panels []struct {
				Title   string
				Targets []struct{ Expr string }
			}
		}
	}
	if err := wait(ctx, func() error {
		return s.request(ctx, http.MethodGet, s.grafana+"/api/dashboards/uid/job-scheduler", nil, &dashboard)
	}); err != nil {
		return err
	}
	if !dashboard.Meta.Provisioned || len(dashboard.Dashboard.Panels) == 0 {
		return fmt.Errorf("scheduler dashboard was not provisioned")
	}
	proxy := s.grafana + "/api/datasources/proxy/uid/scheduler-prometheus/api/v1/query"
	if err := wait(ctx, func() error {
		for expression, baseline := range before {
			values, err := s.query(ctx, proxy, expression)
			if err != nil {
				return err
			}
			if values[0] < baseline+2 {
				return fmt.Errorf("waiting for two new observations of %s", expression)
			}
		}
		for _, panel := range dashboard.Dashboard.Panels {
			for _, target := range panel.Targets {
				expression := strings.ReplaceAll(target.Expr, "$__rate_interval", "1m")
				if _, err := s.query(ctx, proxy, expression); err != nil {
					return fmt.Errorf("panel %s: %w", panel.Title, err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	fmt.Printf("PASS: expected run outcomes and all %d provisioned panels have finite live data.\n", len(dashboard.Dashboard.Panels))
	return nil
}
