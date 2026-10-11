package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func testConfig() Config {
	return Config{DatabaseURL: "postgres://localhost/scheduler", KafkaBrokers: []string{"localhost:9092"}}
}

func TestConfigAndRegistration(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.DatabaseURL = " " },
		func(c *Config) { c.KafkaBrokers = nil },
		func(c *Config) { c.KafkaBrokers = []string{" "} },
		func(c *Config) { c.Concurrency = -1 },
		func(c *Config) { c.Concurrency = 1001 },
	} {
		config := testConfig()
		mutate(&config)
		if _, err := New(config); err == nil {
			t.Fatalf("accepted invalid configuration: %+v", config)
		}
	}
	config := testConfig()
	config.HTTPAllowedHosts = []string{"echo"}
	w, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	config.KafkaBrokers[0], config.HTTPAllowedHosts[0] = "changed", "changed"
	if w.config.Concurrency != 10 || w.config.Logger == nil || w.config.HTTPAddr != "" ||
		w.config.KafkaBrokers[0] != "localhost:9092" || w.config.HTTPAllowedHosts[0] != "echo" {
		t.Fatalf("defaults or configuration ownership: %+v", w.config)
	}
	handler := func(context.Context, Job) error { return nil }
	for _, name := range []string{"", " \t", "noop", "sleep", "fail"} {
		if err := w.Register(name, handler); err == nil {
			t.Fatalf("accepted handler name %q", name)
		}
	}
	if err := w.Register("custom", nil); err == nil {
		t.Fatal("accepted nil callback")
	}
	if err := w.Register(" custom ", handler); err != nil {
		t.Fatal(err)
	}
	if err := w.Register("custom", handler); err == nil {
		t.Fatal("accepted duplicate handler")
	}
}

func TestRunIsOneShotAndConcurrentSafe(t *testing.T) {
	w, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const calls = 32
	results := make(chan error, calls)
	var started sync.WaitGroup
	for range calls {
		started.Go(func() { results <- w.Run(ctx) })
	}
	started.Wait()
	cancelled := 0
	for range calls {
		err := <-results
		if errors.Is(err, context.Canceled) {
			cancelled++
		} else if err == nil || !strings.Contains(err.Error(), "only be called once") {
			t.Fatalf("unexpected Run result: %v", err)
		}
	}
	if cancelled != 1 {
		t.Fatalf("%d calls started; want one", cancelled)
	}
	if err := w.Register("late", func(context.Context, Job) error { return nil }); err == nil {
		t.Fatal("accepted registration after startup")
	}
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("accepted repeated Run")
	}
	w, err = New(Config{DatabaseURL: "://invalid", KafkaBrokers: []string{"localhost:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("invalid database URL started")
	}
	if err := w.Register("late", func(context.Context, Job) error { return nil }); err == nil {
		t.Fatal("accepted registration after failed startup")
	}
}

func TestHandlerAdapterAndPermanent(t *testing.T) {
	var logs bytes.Buffer
	config := testConfig()
	config.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	w, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	message := store.DispatchMessage{
		RunID: "run", JobID: "job", Attempt: 2, FireTime: time.Now(), TimeoutMS: 700,
		Payload: json.RawMessage(`{"handler":"custom","nested":{"value":42},"error":false}`),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sentinel := errors.New("permanent problem")
	callback := w.adapt("custom", func(gotCtx context.Context, job Job) error {
		if gotCtx != ctx || job.RunID != message.RunID || job.JobID != message.JobID ||
			job.Attempt != message.Attempt || !job.FireTime.Equal(message.FireTime) ||
			job.TimeoutMS != message.TimeoutMS || !bytes.Equal(job.Payload, message.Payload) {
			t.Fatalf("callback arguments: %+v", job)
		}
		// Callbacks may retain or edit their payload without changing the
		// executor's original dispatch (used for failure/DLQ records).
		job.Payload[0] = '['
		return fmt.Errorf("wrapped: %w", Permanent(sentinel))
	})
	if err := callback(ctx, message); !errors.Is(err, sentinel) {
		t.Fatalf("Permanent lost wrapped error: %v", err)
	}
	if message.Payload[0] != '{' {
		t.Fatal("callback changed original dispatch payload")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) is not nil")
	}
	underlying := &json.SyntaxError{Offset: 5}
	var target *json.SyntaxError
	if !errors.As(Permanent(underlying), &target) || target != underlying {
		t.Fatal("Permanent lost error type")
	}
	panicHandler := w.adapt("custom", func(context.Context, Job) error { panic("boom") })
	if err := panicHandler(ctx, message); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic error = %v", err)
	}
	for _, text := range []string{"handler panic", "boom", `"run_id":"run"`, `"attempt":2`, "goroutine", "worker_test.go"} {
		if !strings.Contains(logs.String(), text) {
			t.Fatalf("panic log lacks %q: %s", text, logs.String())
		}
	}
}
