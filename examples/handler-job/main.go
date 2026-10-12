// Command handler-job runs a custom worker, or schedules a greeting with
// -schedule. All workers in its consumer group must register "greet".
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zishan044/distributed-job-scheduler/pkg/api"
	"github.com/zishan044/distributed-job-scheduler/pkg/client"
	"github.com/zishan044/distributed-job-scheduler/pkg/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	schedule := flag.Bool("schedule", false, "schedule a greeting and wait instead of running a worker")
	baseURL := flag.String("api-url", "http://localhost:8080", "scheduler API URL for -schedule")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *schedule {
		return scheduleGreeting(ctx, *baseURL)
	}
	concurrency := 0
	if value := os.Getenv("EXECUTOR_CONCURRENCY"); value != "" {
		var err error
		concurrency, err = strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("EXECUTOR_CONCURRENCY: %w", err)
		}
	}
	allowedHosts := strings.Split(os.Getenv("HTTP_ALLOWED_HOSTS"), ",")
	for i := range allowedHosts {
		allowedHosts[i] = strings.TrimSpace(allowedHosts[i])
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "executor")
	w, err := worker.New(worker.Config{
		DatabaseURL: os.Getenv("DATABASE_URL"), KafkaBrokers: strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		Concurrency: concurrency, HTTPAllowedHosts: allowedHosts, HTTPAddr: os.Getenv("HTTP_ADDR"), Logger: logger,
	})
	if err != nil {
		return err
	}
	if err := w.Register("greet", func(ctx context.Context, job worker.Job) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var payload struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return worker.Permanent(err)
		}
		if strings.TrimSpace(payload.Message) == "" {
			return worker.Permanent(errors.New("greet requires a message"))
		}
		logger.InfoContext(ctx, "greeting delivered", "run_id", job.RunID, "attempt", job.Attempt, "message", payload.Message)
		return ctx.Err()
	}); err != nil {
		return err
	}
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func scheduleGreeting(ctx context.Context, baseURL string) error {
	key := os.Getenv("API_KEY")
	if key == "" {
		key = "scheduler-local"
	}
	c, err := client.New(baseURL, client.WithAPIKey(key))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	when := time.Now().UTC()
	job, err := c.ScheduleHandler(ctx, api.CreateJobRequest{
		Name: "handler-job", ScheduledFor: &when, Payload: json.RawMessage(`{"message":"hello from the SDK"}`),
	}, "greet")
	if err != nil {
		return err
	}
	runs, err := c.ListRuns(ctx, job.ID, 1, 0)
	if err != nil {
		return err
	}
	if len(runs) != 1 {
		return fmt.Errorf("job %s has no initial run", job.ID)
	}
	run, err := c.Wait(ctx, runs[0].ID)
	if err != nil {
		return err
	}
	fmt.Printf("job=%s run=%s status=%s\n", job.ID, run.ID, run.Status)
	return nil
}
