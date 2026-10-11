// Package worker runs registered handlers through the scheduler's reliable
// executor. Every worker in the shared "executors" consumer group must register
// the same custom handlers. Handlers must honor context cancellation: Go cannot
// forcibly stop a callback that ignores its context.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

// Config supplies dependencies and capacity. Lease and drain timings retain
// their production defaults (5s heartbeat, 15s lease, 1s margin, 30s drain).
type Config struct {
	DatabaseURL      string
	KafkaBrokers     []string
	Concurrency      int          // Zero defaults to 10; otherwise must be 1–1000.
	HTTPAllowedHosts []string     // Explicit exceptions to HTTP execution's SSRF guard.
	HTTPAddr         string       // Health/metrics listener; empty disables serving.
	Logger           *slog.Logger // Nil uses slog.Default().
}

// Job describes a claimed attempt. Payload is the complete flat JSON object,
// including its "handler" field. TimeoutMS is expressed in milliseconds.
type Job struct {
	RunID     string
	JobID     string
	Attempt   int
	FireTime  time.Time
	TimeoutMS int
	Payload   json.RawMessage
}

// Handler executes an attempt. Return nil on success, an error to retry, or
// Permanent(err) to stop retries. Honor ctx's deadline and cancellation.
type Handler func(ctx context.Context, job Job) error

// Worker owns its connections and handler registry. It can be run only once.
type Worker struct {
	config   Config
	mu       sync.Mutex
	started  bool
	handlers map[string]Handler
}

// New validates configuration without connecting to external services.
func New(config Config) (*Worker, error) {
	config.DatabaseURL = strings.TrimSpace(config.DatabaseURL)
	if config.DatabaseURL == "" {
		return nil, fmt.Errorf("database URL is required")
	}
	config.KafkaBrokers = slices.Clone(config.KafkaBrokers)
	if len(config.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}
	for i, broker := range config.KafkaBrokers {
		config.KafkaBrokers[i] = strings.TrimSpace(broker)
		if config.KafkaBrokers[i] == "" {
			return nil, fmt.Errorf("kafka broker cannot be blank")
		}
	}
	if config.Concurrency == 0 {
		config.Concurrency = 10
	}
	if config.Concurrency < 1 || config.Concurrency > 1000 {
		return nil, fmt.Errorf("concurrency must be between 1 and 1000")
	}
	config.HTTPAllowedHosts = slices.Clone(config.HTTPAllowedHosts)
	config.HTTPAddr = strings.TrimSpace(config.HTTPAddr)
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Worker{config: config, handlers: make(map[string]Handler)}, nil
}

// Register installs a custom handler before Run starts. Names are trimmed;
// blank, duplicate, and reserved names (noop, sleep, fail) are rejected.
func (w *Worker) Register(name string, handler Handler) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return fmt.Errorf("cannot register handlers after Run starts")
	}
	name = strings.TrimSpace(name)
	if name == "" || handler == nil {
		return fmt.Errorf("handler requires a nonblank name and callback")
	}
	if name == "noop" || name == "sleep" || name == "fail" {
		return fmt.Errorf("handler name %q is reserved", name)
	}
	if _, exists := w.handlers[name]; exists {
		return fmt.Errorf("handler %q is already registered", name)
	}
	w.handlers[name] = handler
	return nil
}

// Permanent marks err as non-retryable while preserving errors.Is/As. Nil
// remains nil. Wrapping the returned error preserves its classification.
func Permanent(err error) error { return executor.Permanent(err) }

// Run connects to Postgres and Kafka and consumes jobs.run in the "executors"
// group. Cancellation stops intake, then drains active attempts for up to 30s
// before cancelling their contexts. Run waits for callbacks and resource
// cleanup, returning context.Canceled on cancellation. Even failed starts
// consume the instance; subsequent or concurrent calls are rejected.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return fmt.Errorf("worker Run can only be called once")
	}
	w.started = true
	options := executor.DefaultOptions(w.config.Concurrency)
	options.Handlers = make(map[string]func(context.Context, store.DispatchMessage) error, len(w.handlers))
	for name, handler := range w.handlers {
		options.Handlers[name] = w.adapt(name, handler)
	}
	w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := store.New(ctx, w.config.DatabaseURL)
	if err != nil {
		return err
	}
	defer data.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: w.config.KafkaBrokers, Topic: "jobs.run", GroupID: "executors", CommitInterval: 0,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			w.config.Logger.Error("close Kafka reader", "error", err)
		}
	}()
	dlq := &kafka.Writer{
		Addr: kafka.TCP(w.config.KafkaBrokers...), Topic: "jobs.dlq", Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond,
		// Close waits for delivery; retain the production bounds on retries.
		MaxAttempts: 1, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	}
	defer func() {
		if err := dlq.Close(); err != nil {
			w.config.Logger.Error("close DLQ writer", "error", err)
		}
	}()
	client := executor.HTTPClient(w.config.HTTPAllowedHosts)
	defer client.CloseIdleConnections()
	if w.config.HTTPAddr == "" {
		return executor.Consume(ctx, reader, data, client, dlq, w.config.Logger, options)
	}

	consumeCtx, stopConsume := context.WithCancel(ctx)
	defer stopConsume()
	// Keep metrics available while attempts drain, then join HTTP shutdown.
	serveCtx, stopServe := context.WithCancel(context.WithoutCancel(ctx))
	defer stopServe()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpserver.ServeContext(serveCtx, w.config.HTTPAddr,
			httpserver.NewRouter("executor", w.config.Logger, data.Metrics.Registry), w.config.Logger)
	}()
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- executor.Consume(consumeCtx, reader, data, client, dlq, w.config.Logger, options)
	}()
	select {
	case err := <-serveErr:
		stopConsume()
		consumeResult := <-consumeErr
		if errors.Is(consumeResult, context.Canceled) {
			consumeResult = nil
		}
		return errors.Join(err, consumeResult)
	case err := <-consumeErr:
		stopServe()
		return errors.Join(err, <-serveErr)
	}
}

func (w *Worker) adapt(name string, handler Handler) func(context.Context, store.DispatchMessage) error {
	return func(ctx context.Context, message store.DispatchMessage) (err error) {
		defer func() {
			if value := recover(); value != nil {
				err = fmt.Errorf("handler %q panicked: %v", name, value)
				w.config.Logger.Error("handler panic", "handler", name, "run_id", message.RunID,
					"attempt", message.Attempt, "error", err, "stack", string(debug.Stack()))
			}
		}()
		return handler(ctx, Job{
			RunID: message.RunID, JobID: message.JobID, Attempt: message.Attempt,
			FireTime: message.FireTime, TimeoutMS: message.TimeoutMS, Payload: slices.Clone(message.Payload),
		})
	}
}
