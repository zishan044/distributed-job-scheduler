package main

import (
	"context"
	"encoding/json"
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
)

const dispatchBatchSize = 1000

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

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpserver.Serve(config.HTTPAddr(), httpserver.NewRouter("watcher", logger), logger)
	}()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for {
			count, err := data.DispatchDue(ctx, dispatchBatchSize, func(batch []store.DispatchMessage) error {
				messages := make([]kafka.Message, 0, len(batch))
				for _, item := range batch {
					value, err := json.Marshal(item)
					if err != nil {
						return err
					}
					messages = append(messages, kafka.Message{Key: []byte(item.RunID), Value: value})
				}
				return writer.WriteMessages(ctx, messages...)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("dispatch tick failed", "error", err)
			}
			if count < dispatchBatchSize || err != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-serveErr:
			return err
		case <-ticker.C:
		}
	}
}
