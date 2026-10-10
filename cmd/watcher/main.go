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
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/logging"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/internal/watcher"
)

func main() {
	logger := logging.New("watcher")
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	data, err := store.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer data.Close()
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "jobs.run",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
	}
	defer writer.Close()
	dlq := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: "jobs.dlq", Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond,
	}
	defer dlq.Close()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpserver.Serve(config.HTTPAddr(), httpserver.NewRouter("watcher", logger, data.Metrics.Registry), logger)
	}()
	watchErr := make(chan error, 1)
	go func() { watchErr <- watcher.Run(ctx, data, writer, dlq, logger, watcher.DefaultOptions()) }()
	select {
	case err := <-serveErr:
		stop()
		<-watchErr
		return err
	case err := <-watchErr:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}
