package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func TestReliabilityHandlers(t *testing.T) {
	for _, tc := range []struct {
		payload              string
		wantError, permanent bool
	}{
		{`{"handler":"sleep","duration_ms":1}`, false, false},
		{`{"handler":"sleep"}`, true, true},
		{`{"handler":"sleep","duration_ms":-1}`, true, true},
		{`{"handler":"sleep","duration_ms":9223372036854775807}`, true, true},
		{`{"handler":"sleep","duration_ms":"invalid"}`, true, true},
		{`{"handler":"fail"}`, true, false},
		{`{"handler":"fail","error":"broken","permanent":true}`, true, true},
	} {
		err := Execute(context.Background(), nil, store.DispatchMessage{Type: "handler", Payload: json.RawMessage(tc.payload), TimeoutMS: 1000})
		if (err != nil) != tc.wantError || (err != nil && classify(err).Permanent != tc.permanent) {
			t.Fatalf("Execute(%s) = %v", tc.payload, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, handler := range []string{"sleep", "fail", "noop"} {
		err := Execute(ctx, nil, store.DispatchMessage{Type: "handler", Payload: json.RawMessage(fmt.Sprintf(`{"handler":%q,"duration_ms":60000}`, handler))})
		if !errors.Is(err, context.Canceled) || classify(err).Permanent {
			t.Fatalf("cancelled %s = %v", handler, err)
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	err := Execute(ctx, nil, store.DispatchMessage{Type: "handler", Payload: json.RawMessage(`{"handler":"sleep","duration_ms":60000}`)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sleep timeout = %v", err)
	}
}

func TestRegisteredHandlerDispatch(t *testing.T) {
	message := store.DispatchMessage{Type: "handler", Payload: json.RawMessage(`{"handler":"custom","duration_ms":"custom schema","error":false,"permanent":42}`), TimeoutMS: 20}
	called := false
	handlers := map[string]func(context.Context, store.DispatchMessage) error{
		"custom": func(ctx context.Context, got store.DispatchMessage) error {
			called = true
			if string(got.Payload) != string(message.Payload) {
				t.Fatal("custom payload changed")
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("custom handler has no timeout")
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	if err := execute(context.Background(), nil, message, handlers); !errors.Is(err, context.DeadlineExceeded) || classify(err).Permanent || !called {
		t.Fatalf("custom timeout = %v, called %v", err, called)
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	called = false
	if err := execute(ctx, nil, message, handlers); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("cancelled execution = %v, called %v", err, called)
	}
	ctx, stop = context.WithCancel(context.Background())
	handlers["custom"] = func(ctx context.Context, _ store.DispatchMessage) error {
		stop()
		<-ctx.Done()
		return ctx.Err()
	}
	if err := execute(ctx, nil, message, handlers); !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight cancellation = %v", err)
	}
	sentinel := errors.New("stop retrying")
	handlers["custom"] = func(context.Context, store.DispatchMessage) error {
		return fmt.Errorf("outer: %w", Permanent(sentinel))
	}
	if err := execute(context.Background(), nil, message, handlers); !errors.Is(err, sentinel) || !classify(err).Permanent {
		t.Fatalf("wrapped permanent failure = %v", err)
	}
	message.Payload = json.RawMessage(`{"handler":"noop"}`)
	handlers["noop"] = func(context.Context, store.DispatchMessage) error { t.Fatal("builtin overridden"); return nil }
	if err := execute(context.Background(), nil, message, handlers); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionFailureClassification(t *testing.T) {
	for _, status := range []int{200, 204, 301, 400, 401, 403, 404, 408, 409, 429, 500, 503, 599, 600} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			payload, _ := json.Marshal(httpPayload{URL: server.URL})
			err := Execute(context.Background(), HTTPClient([]string{"127.0.0.1"}), store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 1000})
			if status >= 200 && status < 300 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected execution failure")
			}
			wantPermanent := status != 429 && (status < 500 || status > 599)
			if got := classify(err); got.Permanent != wantPermanent || got.Error == "" {
				t.Fatalf("classify(%v) = %+v; permanent want %v", err, got, wantPermanent)
			}
		})
	}

	for _, tc := range []struct {
		name, jobType, payload string
	}{
		{"invalid handler JSON", "handler", `{`},
		{"unknown handler", "handler", `{"handler":"missing"}`},
		{"invalid HTTP payload", "http", `[]`},
		{"invalid URL", "http", `{"url":"file:///etc/passwd"}`},
		{"invalid method", "http", `{"url":"http://example.com","method":"bad method"}`},
		{"invalid header name", "http", `{"url":"http://example.com","headers":{"bad header":"ok"}}`},
		{"invalid header value", "http", `{"url":"http://example.com","headers":{"X-Test":"bad\r\nvalue"}}`},
		{"blocked target", "http", `{"url":"http://127.0.0.1:1"}`},
		{"unknown type", "other", `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Execute(context.Background(), HTTPClient(nil), store.DispatchMessage{Type: tc.jobType, Payload: json.RawMessage(tc.payload), TimeoutMS: 1000})
			if err == nil || !classify(err).Permanent {
				t.Fatalf("execution error = %v; want permanent failure", err)
			}
		})
	}

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			time.Sleep(100 * time.Millisecond)
		}))
		defer server.Close()
		payload, _ := json.Marshal(httpPayload{URL: server.URL})
		err := Execute(context.Background(), HTTPClient([]string{"127.0.0.1"}), store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 10})
		if err == nil || classify(err).Permanent {
			t.Fatalf("execution error = %v; want retryable timeout", err)
		}
	})
	t.Run("network error", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		server.Close()
		payload, _ := json.Marshal(httpPayload{URL: server.URL})
		err := Execute(context.Background(), HTTPClient([]string{"127.0.0.1"}), store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 1000})
		if err == nil || classify(err).Permanent {
			t.Fatalf("execution error = %v; want retryable network error", err)
		}
	})
	t.Run("oversized response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
		}))
		defer server.Close()
		payload, _ := json.Marshal(httpPayload{URL: server.URL})
		err := Execute(context.Background(), HTTPClient([]string{"127.0.0.1"}), store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 1000})
		if err == nil || !classify(err).Permanent {
			t.Fatalf("execution error = %v; want permanent oversized response", err)
		}
	})
}

func TestDecodeDispatch(t *testing.T) {
	valid := store.DispatchMessage{
		RunID: uuid.NewString(), JobID: uuid.NewString(), Attempt: 1,
		Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`),
		TimeoutMS: 1000, FireTime: time.Now(),
	}
	for _, tc := range []struct {
		name   string
		mutate func(*store.DispatchMessage)
	}{
		{"valid", func(*store.DispatchMessage) {}},
		{"invalid run ID", func(m *store.DispatchMessage) { m.RunID = "bad" }},
		{"nil job ID", func(m *store.DispatchMessage) { m.JobID = uuid.Nil.String() }},
		{"zero attempt", func(m *store.DispatchMessage) { m.Attempt = 0 }},
		{"overflowing attempt", func(m *store.DispatchMessage) { m.Attempt = 1 << 31 }},
		{"zero timeout", func(m *store.DispatchMessage) { m.TimeoutMS = 0 }},
		{"overflowing timeout", func(m *store.DispatchMessage) { m.TimeoutMS = 1 << 31 }},
		{"missing fire time", func(m *store.DispatchMessage) { m.FireTime = time.Time{} }},
		{"unsupported type", func(m *store.DispatchMessage) { m.Type = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := valid
			tc.mutate(&message)
			value, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			_, err = decode(value)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("decode() error = %v", err)
			}
		})
	}
	for _, value := range []string{"{", "null", "{}"} {
		if _, err := decode([]byte(value)); err == nil {
			t.Fatalf("decode(%q) accepted malformed dispatch", value)
		}
	}
}
