// Package client provides a standard-library-only scheduler client.
//
// Schedule methods return job definitions. Use ListRuns to obtain an initial
// run ID before calling Wait; RunNow returns a run directly. Cron execution
// is materialized by the watcher using the job's misfire policy.
//
// Creates generate an idempotency key per call and reuse it on automatic
// retries. Supply WithIdempotencyKey to replay a create across separate calls.
// Reads, cancellation, pause/resume, deletion, and keyed creates retry network errors and HTTP 5xx;
// RunNow and Retry never retry automatically because each call creates a fresh run.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

// Client calls the scheduler API and is safe for concurrent use.
type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
	attempts   int
}

// Option configures a Client at construction time.
type Option func(*Client) error

// New creates a client for an absolute HTTP(S) base URL, optionally including a
// path prefix. Credentials, queries, and fragments are not allowed in baseURL.
// Defaults are a ten-second HTTP timeout and three total attempts. Redirects
// are disabled, including with a supplied HTTP client, to preserve credentials
// and request semantics. The supplied HTTP client is copied, not modified.
func New(baseURL string, opts ...Option) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, fmt.Errorf("invalid scheduler base URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, fmt.Errorf("scheduler base URL must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	c := &Client{
		baseURL:    strings.TrimRight(parsed.String(), "/"),
		httpClient: &http.Client{Timeout: 10 * time.Second}, attempts: 3,
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("nil client option")
		}
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	transport := *c.httpClient
	transport.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.httpClient = &transport
	return c, nil
}

// WithAPIKey sends the key using Authorization: Bearer. An empty key omits the
// header; the server normally rejects unauthenticated API requests with 401.
func WithAPIKey(key string) Option {
	return func(c *Client) error {
		if err := validHeaderValue(key); err != nil {
			return err
		}
		c.apiKey = strings.TrimSpace(key)
		return nil
	}
}

// WithHTTPClient supplies the transport and timeout used for API requests.
// New copies the client and disables redirects. The caller owns the transport
// and must not mutate it while requests are running.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) error {
		if httpClient == nil {
			return fmt.Errorf("HTTP client must not be nil")
		}
		c.httpClient = httpClient
		return nil
	}
}

// WithRetry sets total attempts for operations safe to repeat; 1 disables
// retries. Delays start at 100ms, double up to one second, and honor context
// cancellation. RunNow and Retry always use one attempt regardless of this option.
func WithRetry(attempts int) Option {
	return func(c *Client) error {
		if attempts < 1 {
			return fmt.Errorf("retry attempts must be positive")
		}
		c.attempts = attempts
		return nil
	}
}

type createOptions struct{ key string }

// CreateOption configures a single create call.
type CreateOption func(*createOptions) error

// WithIdempotencyKey supplies a key to a Schedule method. Reuse it only for the
// same logical create; changed batch requests return 409. Keys for single-job
// and batch creates are independent. Surrounding whitespace is trimmed.
func WithIdempotencyKey(key string) CreateOption {
	trimmed := strings.TrimSpace(key)
	return func(opts *createOptions) error {
		if err := validHeaderValue(key); err != nil {
			return err
		}
		if trimmed == "" {
			return fmt.Errorf("idempotency key must not be blank")
		}
		opts.key = trimmed
		return nil
	}
}

func validHeaderValue(value string) error {
	for _, c := range value {
		if (c < 32 && c != '\t') || c == 127 {
			return fmt.Errorf("header value contains control characters")
		}
	}
	return nil
}

func createKey(opts []CreateOption) (string, error) {
	var options createOptions
	for _, opt := range opts {
		if opt == nil {
			return "", fmt.Errorf("nil create option")
		}
		if err := opt(&options); err != nil {
			return "", err
		}
	}
	if options.key == "" {
		options.key = rand.Text()
	}
	return options.key, nil
}

// Schedule creates a job. Validation and scheduling defaults belong to the
// server. A one-shot request needs ScheduledFor; creating a job does not return
// its initial run ID.
func (c *Client) Schedule(ctx context.Context, request api.CreateJobRequest, opts ...CreateOption) (api.Job, error) {
	key, err := createKey(opts)
	if err != nil {
		return api.Job{}, err
	}
	var job api.Job
	err = c.request(ctx, http.MethodPost, "/v1/jobs", request, &job, key, true)
	return job, err
}

// ScheduleHTTP sets Type and Payload for an HTTP job, preserving the other
// request fields. The executor owns HTTP target validation and egress safety.
func (c *Client) ScheduleHTTP(ctx context.Context, request api.CreateJobRequest, payload api.HTTPPayload, opts ...CreateOption) (api.Job, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return api.Job{}, err
	}
	request.Type, request.Payload = "http", encoded
	return c.Schedule(ctx, request, opts...)
}

// ScheduleHandler sets Type and adds handler to the existing flat payload,
// replacing any handler field. Payload must be a JSON object or omitted. The
// handler name must match a registration on every executor in the group.
func (c *Client) ScheduleHandler(ctx context.Context, request api.CreateJobRequest, handler string, opts ...CreateOption) (api.Job, error) {
	handler = strings.TrimSpace(handler)
	if handler == "" {
		return api.Job{}, fmt.Errorf("handler name must not be blank")
	}
	payload := make(map[string]json.RawMessage)
	if len(request.Payload) > 0 {
		if err := json.Unmarshal(request.Payload, &payload); err != nil {
			return api.Job{}, fmt.Errorf("decode handler payload: %w", err)
		}
		if payload == nil {
			return api.Job{}, fmt.Errorf("handler payload must be a JSON object")
		}
	}
	name, err := json.Marshal(handler)
	if err != nil {
		return api.Job{}, err
	}
	payload["handler"] = name
	encoded, err := json.Marshal(payload)
	if err != nil {
		return api.Job{}, err
	}
	request.Type, request.Payload = "handler", encoded
	return c.Schedule(ctx, request, opts...)
}

// ScheduleBatch creates 1–100 jobs atomically and returns them in input order.
// Automatic retries reuse the same batch body and idempotency key.
func (c *Client) ScheduleBatch(ctx context.Context, requests []api.CreateJobRequest, opts ...CreateOption) ([]api.Job, error) {
	if len(requests) < 1 || len(requests) > api.MaxBatchSize {
		return nil, fmt.Errorf("batch must contain 1–100 jobs")
	}
	key, err := createKey(opts)
	if err != nil {
		return nil, err
	}
	var jobs []api.Job
	err = c.request(ctx, http.MethodPost, "/v1/jobs/batch", api.CreateBatchRequest{Jobs: requests}, &jobs, key, true)
	return jobs, err
}

// GetJob retrieves a job definition by job ID.
func (c *Client) GetJob(ctx context.Context, jobID string) (api.Job, error) {
	var job api.Job
	err := c.request(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID), nil, &job, "", true)
	return job, err
}

// Delete soft-deletes a job, cancels waiting runs, and requests cooperative
// cancellation of running work. History remains readable; repeating Delete is
// harmless and uses the configured transient retry policy.
func (c *Client) Delete(ctx context.Context, jobID string) (api.Job, error) {
	var job api.Job
	err := c.request(ctx, http.MethodDelete, "/v1/jobs/"+url.PathEscape(jobID), nil, &job, "", true)
	return job, err
}

// Pause stops new cron fires. Existing runs and RunNow remain available, and
// the cursor is preserved. One-shot and deleted jobs cannot be paused.
func (c *Client) Pause(ctx context.Context, jobID string) (api.Job, error) {
	var job api.Job
	err := c.request(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/pause", nil, &job, "", true)
	return job, err
}

// Resume enables cron materialization; missed fires follow the job's misfire
// policy. Pause and Resume retry transient failures using WithRetry settings.
func (c *Client) Resume(ctx context.Context, jobID string) (api.Job, error) {
	var job api.Job
	err := c.request(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/resume", nil, &job, "", true)
	return job, err
}

// ListRuns returns runs newest first. Limit is 1–100, or 0 for the server's
// default of 50; offset must be non-negative.
func (c *Client) ListRuns(ctx context.Context, jobID string, limit, offset int) ([]api.Run, error) {
	if limit < 0 || limit > 100 || offset < 0 {
		return nil, fmt.Errorf("run limit must be 0–100 and offset must be non-negative")
	}
	query := url.Values{"offset": {strconv.Itoa(offset)}}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	var runs []api.Run
	err := c.request(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID)+"/runs?"+query.Encode(), nil, &runs, "", true)
	return runs, err
}

// Get retrieves an execution run by run ID, not a job definition.
func (c *Client) Get(ctx context.Context, runID string) (api.Run, error) {
	var run api.Run
	err := c.request(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID), nil, &run, "", true)
	return run, err
}

// RunNow creates a fresh run due immediately. It never retries automatically:
// a network error can mean the server created a run but lost its response.
func (c *Client) RunNow(ctx context.Context, jobID string) (api.Run, error) {
	var run api.Run
	err := c.request(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/run-now", nil, &run, "", false)
	return run, err
}

// Cancel cancels a pending/queued run or requests cooperative cancellation of
// a running attempt. A successful response need not yet have status CANCELLED.
func (c *Client) Cancel(ctx context.Context, runID string) (api.Run, error) {
	var run api.Run
	err := c.request(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/cancel", nil, &run, "", true)
	return run, err
}

// Retry creates a fresh run at attempt 1 linked to a DEAD source through
// RetryOf. Each call starts another chain and leaves the source unchanged.
// It never retries automatically: a lost response may hide a successful create.
func (c *Client) Retry(ctx context.Context, runID string) (api.Run, error) {
	var run api.Run
	err := c.request(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/retry", nil, &run, "", false)
	return run, err
}

// Wait polls a run every 500ms until terminal or ctx ends. Only SUCCEEDED
// returns nil error. Other terminal states return the run and a *TerminalError;
// lookup errors or cancellation return the last observed run and the error.
func (c *Client) Wait(ctx context.Context, runID string) (api.Run, error) {
	var last api.Run
	for {
		run, err := c.Get(ctx, runID)
		if err != nil {
			return last, err
		}
		last = run
		switch run.Status {
		case api.StatusSucceeded:
			return run, nil
		case api.StatusDead, api.StatusCancelled, api.StatusFailed:
			return run, &TerminalError{Run: run}
		}
		if err := wait(ctx, 500*time.Millisecond); err != nil {
			return last, err
		}
	}
}

// HTTPError describes a non-2xx API response. Message contains the JSON error
// field, a bounded text response, or the HTTP status text if the body is empty.
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("scheduler API: HTTP %d: %s", e.StatusCode, e.Message)
}

// TerminalError reports an unsuccessful completed run, including its final
// status, attempt, and LastError. Inspect it using errors.As.
type TerminalError struct{ Run api.Run }

func (e *TerminalError) Error() string {
	message := fmt.Sprintf("run %s finished with status %s", e.Run.ID, e.Run.Status)
	if e.Run.LastError != "" {
		message += ": " + e.Run.LastError
	}
	return message
}

func (c *Client) request(ctx context.Context, method, path string, input, output any, key string, safe bool) error {
	var body []byte
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode scheduler request: %w", err)
		}
		body = encoded
	}
	attempts := 1
	if safe {
		attempts = c.attempts
	}
	delay := 100 * time.Millisecond
	var err error
	for attempt := range attempts {
		if attempt > 0 {
			if err := wait(ctx, delay); err != nil {
				return err
			}
			delay = min(delay*2, time.Second)
		}
		var retry bool
		retry, err = c.requestOnce(ctx, method, path, body, output, key)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil || !retry {
			return err
		}
	}
	return err
}

func (c *Client) requestOnce(ctx context.Context, method, path string, body []byte, output any, key string) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return true, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Bound error text to 8 KiB.
		body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		var failure api.ErrorResponse
		message := strings.TrimSpace(string(body))
		if json.Unmarshal(body, &failure) == nil && failure.Error != "" {
			message = failure.Error
		}
		if message == "" {
			message = http.StatusText(response.StatusCode)
		}
		return response.StatusCode >= 500 && response.StatusCode <= 599,
			&HTTPError{StatusCode: response.StatusCode, Message: message}
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return true, fmt.Errorf("read scheduler response: %w", err)
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return false, fmt.Errorf("scheduler response must not be null")
	}
	if err := json.Unmarshal(data, output); err != nil {
		return false, fmt.Errorf("decode scheduler response: %w", err)
	}
	return false, nil
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
