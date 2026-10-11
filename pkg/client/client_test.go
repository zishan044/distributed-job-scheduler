package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zishan044/distributed-job-scheduler/pkg/api"
	"github.com/zishan044/distributed-job-scheduler/pkg/client"
)

func newClient(t *testing.T, baseURL string, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.New(baseURL, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}

func scheduledRequest() api.CreateJobRequest {
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	return api.CreateJobRequest{
		Name: "demo", Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`), ScheduledFor: &at,
	}
}

func TestRoutesAuthenticationAndPayloads(t *testing.T) {
	request := scheduledRequest()
	httpPayload := api.HTTPPayload{URL: "https://example.com/reminder", Method: "POST", Headers: map[string]string{"X-Demo": "yes"}, Body: "hello"}
	handlerRequest := request
	handlerRequest.Type = "http"
	handlerRequest.Payload = json.RawMessage(`{"handler":"old","duration_ms":10,"nested":{"a":true}}`)
	ctx := context.Background()
	for _, tc := range []struct {
		name, method, uri string
		create            bool
		body, response    any
		call              func(*client.Client) (string, error)
	}{
		{"schedule", "POST", "/v1/jobs", true, request, api.Job{ID: "job"}, func(c *client.Client) (string, error) {
			job, err := c.Schedule(ctx, request)
			return job.ID, err
		}},
		{"HTTP", "POST", "/v1/jobs", true, api.CreateJobRequest{
			Name: request.Name, Type: "http", ScheduledFor: request.ScheduledFor,
			Payload: json.RawMessage(`{"url":"https://example.com/reminder","method":"POST","headers":{"X-Demo":"yes"},"body":"hello"}`),
		}, api.Job{ID: "job"}, func(c *client.Client) (string, error) {
			job, err := c.ScheduleHTTP(ctx, request, httpPayload)
			return job.ID, err
		}},
		{"handler", "POST", "/v1/jobs", true, api.CreateJobRequest{
			Name: request.Name, Type: "handler", ScheduledFor: request.ScheduledFor,
			Payload: json.RawMessage(`{"handler":"custom","duration_ms":10,"nested":{"a":true}}`),
		}, api.Job{ID: "job"}, func(c *client.Client) (string, error) {
			job, err := c.ScheduleHandler(ctx, handlerRequest, "custom")
			return job.ID, err
		}},
		{"batch", "POST", "/v1/jobs/batch", true, api.CreateBatchRequest{Jobs: []api.CreateJobRequest{request, request}}, []api.Job{{ID: "first"}, {ID: "second"}}, func(c *client.Client) (string, error) {
			jobs, err := c.ScheduleBatch(ctx, []api.CreateJobRequest{request, request})
			if len(jobs) != 2 {
				return "", fmt.Errorf("batch returned %d jobs", len(jobs))
			}
			return jobs[0].ID + "," + jobs[1].ID, err
		}},
		{"get job", "GET", "/v1/jobs/job", false, nil, api.Job{ID: "job"}, func(c *client.Client) (string, error) {
			job, err := c.GetJob(ctx, "job")
			return job.ID, err
		}},
		{"list", "GET", "/v1/jobs/job/runs?limit=25&offset=50", false, nil, []api.Run{{ID: "run"}}, func(c *client.Client) (string, error) {
			runs, err := c.ListRuns(ctx, "job", 25, 50)
			if len(runs) != 1 {
				return "", fmt.Errorf("list returned %d runs", len(runs))
			}
			return runs[0].ID, err
		}},
		{"get run", "GET", "/v1/runs/run", false, nil, api.Run{ID: "run"}, func(c *client.Client) (string, error) {
			run, err := c.Get(ctx, "run")
			return run.ID, err
		}},
		{"run now", "POST", "/v1/jobs/job/run-now", false, nil, api.Run{ID: "run"}, func(c *client.Client) (string, error) {
			run, err := c.RunNow(ctx, "job")
			return run.ID, err
		}},
		{"cancel", "POST", "/v1/runs/run/cancel", false, nil, api.Run{ID: "run", Status: api.StatusRunning, CancelRequested: true}, func(c *client.Client) (string, error) {
			run, err := c.Cancel(ctx, "run")
			return run.ID, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != tc.method || r.RequestURI != tc.uri || r.Header.Get("Authorization") != "Bearer test-key" || r.Header.Get("Accept") != "application/json" {
					t.Errorf("request = %s %s headers=%v", r.Method, r.RequestURI, r.Header)
				}
				if (r.Header.Get("Idempotency-Key") != "") != tc.create {
					t.Errorf("unexpected idempotency header: %q", r.Header.Get("Idempotency-Key"))
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if tc.body == nil {
					if len(body) != 0 {
						t.Errorf("unexpected body: %s", body)
					}
				} else {
					if r.Header.Get("Content-Type") != "application/json" {
						t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
					}
					wantBody, err := json.Marshal(tc.body)
					if err != nil {
						t.Error(err)
					}
					var got, want any
					if err := json.Unmarshal(body, &got); err != nil {
						t.Error(err)
					}
					if err := json.Unmarshal(wantBody, &want); err != nil {
						t.Error(err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("body = %s; want %s", body, wantBody)
					}
				}
				if tc.create {
					w.WriteHeader(http.StatusCreated)
				}
				writeJSON(t, w, tc.response)
			}))
			defer server.Close()
			c := newClient(t, server.URL, client.WithAPIKey("test-key"), client.WithRetry(1))
			got, err := tc.call(c)
			want := "run"
			if strings.Contains(tc.name, "job") || tc.create {
				want = "job"
			}
			if tc.name == "batch" {
				want = "first,second"
			}
			if err != nil || got != want || calls.Load() != 1 {
				t.Fatalf("result = %q, %v, calls=%d; want %q", got, err, calls.Load(), want)
			}
		})
	}
	if string(handlerRequest.Payload) != `{"handler":"old","duration_ms":10,"nested":{"a":true}}` || handlerRequest.Type != "http" {
		t.Fatal("handler helper mutated its input")
	}
}

func TestCreateRetriesFreezeBodyAndKey(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			var calls atomic.Int32
			var mu sync.Mutex
			var bodies, keys []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				mu.Lock()
				bodies = append(bodies, string(body))
				keys = append(keys, r.Header.Get("Idempotency-Key"))
				mu.Unlock()
				if calls.Add(1) <= 2 {
					w.WriteHeader(http.StatusServiceUnavailable)
					writeJSON(t, w, api.ErrorResponse{Error: "retry"})
					return
				}
				// Redis and batch replays can return 200 rather than 201.
				if batch {
					writeJSON(t, w, []api.Job{{ID: "job"}})
				} else {
					writeJSON(t, w, api.Job{ID: "job"})
				}
			}))
			defer server.Close()
			c := newClient(t, server.URL)
			var err error
			if batch {
				_, err = c.ScheduleBatch(context.Background(), []api.CreateJobRequest{scheduledRequest()})
			} else {
				_, err = c.Schedule(context.Background(), scheduledRequest())
			}
			if err != nil || calls.Load() != 3 {
				t.Fatalf("create = %v, calls=%d; want three attempts", err, calls.Load())
			}
			mu.Lock()
			defer mu.Unlock()
			for i := range bodies {
				if bodies[i] != bodies[0] || keys[i] == "" || keys[i] != keys[0] {
					t.Fatalf("retry changed body/key: %v, %v", bodies, keys)
				}
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNetworkRetryAndFrozenCallerData(t *testing.T) {
	requests := []api.CreateJobRequest{scheduledRequest()}
	var calls int
	var firstBody, firstKey string
	transportFailure := errors.New("connection reset")
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		key := r.Header.Get("Idempotency-Key")
		if calls == 1 {
			firstBody, firstKey = string(body), key
			requests[0].Name = "changed after first attempt"
			copy(requests[0].Payload, `{"handler":"fail"}`)
		} else if string(body) != firstBody || key != firstKey {
			t.Errorf("network retry changed frozen body/key")
		}
		if calls < 3 {
			return nil, transportFailure
		}
		return &http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(`[{"id":"job"}]`)), Header: make(http.Header)}, nil
	})}
	c := newClient(t, "http://scheduler.example", client.WithHTTPClient(httpClient))
	jobs, err := c.ScheduleBatch(context.Background(), requests, client.WithIdempotencyKey(" explicit-key "))
	if err != nil || len(jobs) != 1 || calls != 3 || firstKey != "explicit-key" {
		t.Fatalf("network retry = %+v, %v, calls=%d, key=%q", jobs, err, calls, firstKey)
	}
}

func TestRunNowNetworkFailureNeverRetries(t *testing.T) {
	var calls int
	networkFailure := errors.New("connection reset after request")
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Header.Get("Idempotency-Key") != "" {
			t.Error("RunNow unexpectedly sent an idempotency key")
		}
		return nil, networkFailure
	})}
	_, err := newClient(t, "http://scheduler.example", client.WithHTTPClient(httpClient)).RunNow(context.Background(), "job")
	if !errors.Is(err, networkFailure) || calls != 1 {
		t.Fatalf("RunNow network failure = %v, calls=%d", err, calls)
	}
}

func TestTruncatedResponsesRetryOnlySafeOperations(t *testing.T) {
	for _, safe := range []bool{true, false} {
		t.Run(fmt.Sprintf("safe=%v", safe), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.Header().Set("Content-Length", "100")
					if _, err := io.WriteString(w, `{"id":`); err != nil {
						t.Error(err)
					}
					return
				}
				writeJSON(t, w, api.Run{ID: "run"})
			}))
			defer server.Close()
			c := newClient(t, server.URL)
			if safe {
				run, err := c.Get(context.Background(), "run")
				if err != nil || run.ID != "run" || calls.Load() != 2 {
					t.Fatalf("truncated GET = %+v, %v, calls=%d", run, err, calls.Load())
				}
			} else {
				_, err := c.RunNow(context.Background(), "job")
				if !errors.Is(err, io.ErrUnexpectedEOF) || calls.Load() != 1 {
					t.Fatalf("truncated RunNow = %v, calls=%d", err, calls.Load())
				}
			}
		})
	}
}

type countedBody struct {
	io.Reader
	closes *atomic.Int32
}

func (b countedBody) Close() error { b.closes.Add(1); return nil }

func TestResponseBodiesClosedOnSuccessAndRetry(t *testing.T) {
	for _, status := range []int{200, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var closes atomic.Int32
			httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: status, Header: make(http.Header),
					Body: countedBody{Reader: strings.NewReader(`{"id":"run"}`), closes: &closes},
				}, nil
			})}
			_, err := newClient(t, "http://scheduler.example", client.WithHTTPClient(httpClient)).Get(context.Background(), "run")
			want := int32(1)
			if status == 503 {
				want = 3
				if err == nil {
					t.Fatal("expected failed response")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if closes.Load() != want {
				t.Fatalf("response closes = %d; want %d", closes.Load(), want)
			}
		})
	}
}

func TestRetryPolicyAndTypedErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body                  string
		status, attempts, wantCalls int
		call                        func(*client.Client) error
	}{
		{"GET 5xx", `{"error":"unavailable"}`, 503, 3, 3, func(c *client.Client) error { _, err := c.Get(context.Background(), "run"); return err }},
		{"cancel 5xx", `{"error":"unavailable"}`, 500, 3, 3, func(c *client.Client) error { _, err := c.Cancel(context.Background(), "run"); return err }},
		{"disabled", `{"error":"unavailable"}`, 503, 1, 1, func(c *client.Client) error { _, err := c.Get(context.Background(), "run"); return err }},
		{"custom limit", `{"error":"unavailable"}`, 503, 2, 2, func(c *client.Client) error { _, err := c.GetJob(context.Background(), "job"); return err }},
		{"run now", `{"error":"unavailable"}`, 503, 3, 1, func(c *client.Client) error { _, err := c.RunNow(context.Background(), "job"); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				if _, err := io.WriteString(w, tc.body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			err := tc.call(newClient(t, server.URL, client.WithRetry(tc.attempts)))
			var failure *client.HTTPError
			if !errors.As(err, &failure) || failure.StatusCode != tc.status || failure.Message != "unavailable" || int(calls.Load()) != tc.wantCalls {
				t.Fatalf("error=%v, calls=%d; want HTTP %d and %d calls", err, calls.Load(), tc.status, tc.wantCalls)
			}
		})
	}
	for _, status := range []int{400, 401, 404, 409, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				if _, err := io.WriteString(w, "plain text error"); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			_, err := newClient(t, server.URL).Schedule(context.Background(), scheduledRequest())
			var failure *client.HTTPError
			if !errors.As(err, &failure) || failure.StatusCode != status || failure.Message != "plain text error" || calls.Load() != 1 {
				t.Fatalf("4xx error=%v, calls=%d", err, calls.Load())
			}
		})
	}
}

func TestCancellationInterruptsRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		time.AfterFunc(25*time.Millisecond, cancel)
	}))
	defer server.Close()
	_, err := newClient(t, server.URL).Schedule(ctx, scheduledRequest())
	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("cancelled retry = %v, calls=%d", err, calls.Load())
	}
}

func TestWaitAcrossAttemptsAndTerminalErrors(t *testing.T) {
	var calls atomic.Int32
	states := []api.Run{
		{ID: "run", Status: api.StatusPending, Attempt: 1},
		{ID: "run", Status: api.StatusRunning, Attempt: 1},
		{ID: "run", Status: api.StatusPending, Attempt: 2},
		{ID: "run", Status: api.StatusSucceeded, Attempt: 2},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := int(calls.Add(1)) - 1
		if index >= len(states) {
			index = len(states) - 1
		}
		writeJSON(t, w, states[index])
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run, err := newClient(t, server.URL).Wait(ctx, "run")
	if err != nil || run.Status != api.StatusSucceeded || run.Attempt != 2 || calls.Load() != 4 {
		t.Fatalf("Wait = %+v, %v, calls=%d", run, err, calls.Load())
	}
	for _, status := range []string{api.StatusDead, api.StatusCancelled, api.StatusFailed} {
		t.Run(status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(t, w, api.Run{ID: "run", Status: status, Attempt: 3, LastError: "demo failure"})
			}))
			defer server.Close()
			run, err := newClient(t, server.URL).Wait(context.Background(), "run")
			var failure *client.TerminalError
			if !errors.As(err, &failure) || run.Status != status || !reflect.DeepEqual(failure.Run, run) || !strings.Contains(err.Error(), "demo failure") {
				t.Fatalf("terminal Wait = %+v, %v", run, err)
			}
		})
	}
}

func TestWaitCancellationAndLookupErrorPreserveLastRun(t *testing.T) {
	for _, cancelWait := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel=%v", cancelWait), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) > 1 {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				writeJSON(t, w, api.Run{ID: "run", Status: api.StatusRunning, Attempt: 2})
				if cancelWait {
					time.AfterFunc(25*time.Millisecond, cancel)
				}
			}))
			defer server.Close()
			run, err := newClient(t, server.URL).Wait(ctx, "run")
			if run.ID != "run" || run.Attempt != 2 {
				t.Fatalf("lost last run: %+v, %v", run, err)
			}
			if cancelWait {
				if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
					t.Fatalf("cancel Wait = %v, calls=%d", err, calls.Load())
				}
			} else {
				var failure *client.HTTPError
				if !errors.As(err, &failure) || failure.StatusCode != 404 || failure.Message != "Not Found" {
					t.Fatalf("lookup error = %v", err)
				}
			}
		})
	}
}

func TestConfigurationAndLocalValidation(t *testing.T) {
	for _, baseURL := range []string{"", "/relative", "ftp://example.com", "http://", "http://user:pass@example.com", "http://example.com?x=1", "http://example.com#fragment"} {
		if _, err := client.New(baseURL); err == nil {
			t.Errorf("accepted base URL %q", baseURL)
		}
	}
	for _, opt := range []client.Option{nil, client.WithRetry(0), client.WithRetry(-1), client.WithHTTPClient(nil), client.WithAPIKey("key\r\ninjected")} {
		if _, err := client.New("http://example.com", opt); err == nil {
			t.Error("accepted invalid client option")
		}
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer server.Close()
	c := newClient(t, server.URL)
	for _, payload := range []string{"[]", "null", `"string"`, "{"} {
		request := scheduledRequest()
		request.Payload = json.RawMessage(payload)
		if _, err := c.ScheduleHandler(context.Background(), request, "custom"); err == nil {
			t.Errorf("accepted handler payload %q", payload)
		}
	}
	if _, err := c.ScheduleHandler(context.Background(), scheduledRequest(), " "); err == nil {
		t.Error("accepted blank handler")
	}
	for _, requests := range [][]api.CreateJobRequest{nil, make([]api.CreateJobRequest, 101)} {
		if _, err := c.ScheduleBatch(context.Background(), requests); err == nil {
			t.Error("accepted invalid batch size")
		}
	}
	for _, opt := range []client.CreateOption{nil, client.WithIdempotencyKey(" "), client.WithIdempotencyKey("key\r\ninjected")} {
		if _, err := c.Schedule(context.Background(), scheduledRequest(), opt); err == nil {
			t.Error("accepted invalid create option")
		}
	}
	for _, bounds := range [][2]int{{-1, 0}, {101, 0}, {1, -1}} {
		if _, err := c.ListRuns(context.Background(), "job", bounds[0], bounds[1]); err == nil {
			t.Error("accepted invalid list bounds")
		}
	}
	request := scheduledRequest()
	request.Payload = json.RawMessage("{")
	if _, err := c.Schedule(context.Background(), request); err == nil {
		t.Error("accepted unencodable payload")
	}
	if calls.Load() != 0 {
		t.Fatalf("local validation made %d requests", calls.Load())
	}
}

func TestPrefixEscapedIDAndDefaultListLimit(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.RequestURI != "/proxy/v1/jobs/job%2Fid%3Fx%23y/runs?offset=0" {
			t.Errorf("RequestURI = %s", r.RequestURI)
		}
		writeJSON(t, w, []api.Run{})
	}))
	defer server.Close()
	runs, err := newClient(t, server.URL+"/proxy/").ListRuns(context.Background(), "job/id?x#y", 0, 0)
	if err != nil || runs == nil || len(runs) != 0 || calls.Load() != 1 {
		t.Fatalf("empty list = %+v, %v", runs, err)
	}
}

func TestRedirectsDisabledWithoutMutatingSuppliedClient(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		writeJSON(t, w, api.Job{ID: "leaked"})
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	supplied := server.Client()
	var customRedirect atomic.Int32
	supplied.CheckRedirect = func(*http.Request, []*http.Request) error { customRedirect.Add(1); return nil }
	c := newClient(t, server.URL, client.WithHTTPClient(supplied), client.WithAPIKey("secret"))
	_, err := c.Schedule(context.Background(), scheduledRequest())
	var failure *client.HTTPError
	if !errors.As(err, &failure) || failure.StatusCode != 307 || redirected.Load() != 0 || customRedirect.Load() != 0 {
		t.Fatalf("redirect = %v, target calls=%d", err, redirected.Load())
	}
	if supplied.CheckRedirect == nil {
		t.Fatal("supplied client modified")
	}
	if err := supplied.CheckRedirect(nil, nil); err != nil || customRedirect.Load() != 1 {
		t.Fatal("supplied redirect callback modified")
	}
}

func TestInvalidSuccessfulResponsesAreNotRetried(t *testing.T) {
	for _, body := range []string{"not JSON", "null", "", `{"id":"job"} {}`} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if _, err := io.WriteString(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			_, err := newClient(t, server.URL).Schedule(context.Background(), scheduledRequest())
			if err == nil || calls.Load() != 1 {
				t.Fatalf("invalid response = %v, calls=%d", err, calls.Load())
			}
		})
	}
}

func TestConcurrentCallsAndReusableCreateOption(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Idempotency-Key") != "shared-key" {
			t.Errorf("key = %q", r.Header.Get("Idempotency-Key"))
		}
		writeJSON(t, w, api.Job{ID: "job"})
	}))
	defer server.Close()
	c, opt := newClient(t, server.URL), client.WithIdempotencyKey(" shared-key ")
	const callers = 8
	errors := make(chan error, callers)
	for range callers {
		go func() { _, err := c.Schedule(context.Background(), scheduledRequest(), opt); errors <- err }()
	}
	for range callers {
		if err := <-errors; err != nil {
			t.Error(err)
		}
	}
	if calls.Load() != callers {
		t.Fatalf("concurrent calls = %d", calls.Load())
	}
}

func TestHandlerWithOmittedPayloadAndDistinctGeneratedKeys(t *testing.T) {
	var mu sync.Mutex
	keys := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request api.CreateJobRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Type != "handler" || !bytes.Equal(request.Payload, []byte(`{"handler":"noop"}`)) {
			t.Errorf("handler request = %+v", request)
		}
		mu.Lock()
		key := r.Header.Get("Idempotency-Key")
		if key == "" || keys[key] {
			t.Errorf("empty or reused generated key: %q", key)
		}
		keys[key] = true
		mu.Unlock()
		writeJSON(t, w, api.Job{ID: "job"})
	}))
	defer server.Close()
	c := newClient(t, server.URL)
	for range 2 {
		request := scheduledRequest()
		request.Payload = nil
		if _, err := c.ScheduleHandler(context.Background(), request, "noop"); err != nil {
			t.Fatal(err)
		}
	}
}
