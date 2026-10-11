package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zishan044/distributed-job-scheduler/internal/config"
	"github.com/zishan044/distributed-job-scheduler/internal/logging"
	"github.com/zishan044/distributed-job-scheduler/pkg/worker"
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
	instance, err := worker.New(worker.Config{
		DatabaseURL: databaseURL, KafkaBrokers: brokers, Concurrency: concurrency,
		HTTPAllowedHosts: config.HTTPAllowedHosts(), HTTPAddr: config.HTTPAddr(), Logger: logger,
	})
	if err != nil {
		return err
	}
	if err := instance.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
