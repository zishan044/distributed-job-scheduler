package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/config"
	"github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/logging"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func main() {
	logger := logging.New("executor")
	logger.Info("starting service")
	if err := run(logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL, err := config.DatabaseURL()
	if err != nil {
		return err
	}
	brokers, err := config.KafkaBrokers()
	if err != nil {
		return err
	}
	concurrency, err := config.ExecutorConcurrency()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	data, err := store.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer data.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "jobs.run",
		GroupID:        "executors",
		CommitInterval: 0,
	})
	defer reader.Close()
	dlq := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: "jobs.dlq", Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond,
	}
	defer dlq.Close()
	client := executor.HTTPClient(config.HTTPAllowedHosts())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpserver.Serve(config.HTTPAddr(), httpserver.NewRouter("executor", logger), logger)
	}()
	if err := executor.Consume(ctx, reader, data, client, dlq, logger, executor.DefaultOptions(concurrency)); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	select {
	case err := <-serveErr:
		return err
	default:
		return nil
	}
}
