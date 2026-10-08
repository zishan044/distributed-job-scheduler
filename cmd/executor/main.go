package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/config"
	"github.com/zishan044/distributed-job-scheduler/internal/executor"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/logging"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

const claimWindow = 5 * time.Millisecond

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
	client := executor.HTTPClient(config.HTTPAllowedHosts())
	workerID, err := uuid.NewV7()
	if err != nil {
		return err
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpserver.Serve(config.HTTPAddr(), httpserver.NewRouter("executor", logger), logger)
	}()
	if err := consume(ctx, reader, data, client, workerID.String(), concurrency, logger); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	select {
	case err := <-serveErr:
		return err
	default:
		return nil
	}
}

func consume(ctx context.Context, reader *kafka.Reader, data *store.Store, client *http.Client, workerID string, concurrency int, logger *slog.Logger) error {
	active := make(chan struct{}, concurrency)
	for {
		select {
		case active <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		first, err := reader.FetchMessage(ctx)
		if err != nil {
			<-active
			return err
		}
		kafkaMessages := []kafka.Message{first}
		dispatchMessages := []store.DispatchMessage{decode(first.Value)}
	collecting:
		for len(kafkaMessages) < concurrency {
			select {
			case active <- struct{}{}:
			default:
				break collecting
			}
			fetchCtx, cancel := context.WithTimeout(ctx, claimWindow)
			message, err := reader.FetchMessage(fetchCtx)
			cancel()
			if err != nil {
				<-active
				break collecting
			}
			kafkaMessages = append(kafkaMessages, message)
			dispatchMessages = append(dispatchMessages, decode(message.Value))
		}
		claimed, err := data.StartRuns(ctx, dispatchMessages, workerID)
		if err != nil {
			for range kafkaMessages {
				<-active
			}
			return err
		}
		if err := reader.CommitMessages(ctx, kafkaMessages...); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("commit Kafka messages failed", "error", err)
		}
		claimedCount := make(map[string]int, len(claimed))
		for _, item := range claimed {
			claimedCount[item.RunID]++
			go func(item store.DispatchMessage) {
				defer func() { <-active }()
				if err := executor.Execute(ctx, client, item); err != nil {
					logger.Error("job execution failed", "run_id", item.RunID, "attempt", item.Attempt, "error", err)
					return
				}
				succeeded, err := data.SucceedRun(ctx, item.RunID, item.Attempt, workerID)
				if err != nil {
					logger.Error("save job result failed", "run_id", item.RunID, "attempt", item.Attempt, "error", err)
				} else if !succeeded {
					logger.Warn("discarded stale job completion", "run_id", item.RunID, "attempt", item.Attempt)
				}
			}(item)
		}
		for _, item := range dispatchMessages {
			if claimedCount[item.RunID] > 0 {
				claimedCount[item.RunID]--
			} else {
				<-active
			}
		}
	}
}

func decode(value []byte) store.DispatchMessage {
	var message store.DispatchMessage
	if err := json.Unmarshal(value, &message); err != nil {
		return store.DispatchMessage{}
	}
	return message
}
