package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/queue"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

type Options struct {
	DispatchInterval  time.Duration
	SweepInterval     time.Duration
	RetentionInterval time.Duration
	QueuedTimeout     time.Duration
	OperationTimeout  time.Duration
	BatchSize         int
}

func DefaultOptions() Options {
	return Options{
		DispatchInterval: 200 * time.Millisecond, SweepInterval: time.Second,
		RetentionInterval: time.Hour,
		QueuedTimeout:     5 * time.Minute, OperationTimeout: 5 * time.Second, BatchSize: 1000,
	}
}

func (o Options) validate() error {
	if o.DispatchInterval <= 0 || o.SweepInterval <= 0 || o.RetentionInterval <= 0 || o.QueuedTimeout <= 0 || o.OperationTimeout <= 0 || o.BatchSize < 1 || o.BatchSize > 2000 {
		return fmt.Errorf("invalid watcher batch size or timings")
	}
	return nil
}

// Run keeps sweeping, cron materialization, and retention independent of Kafka dispatch.
func Run(ctx context.Context, data *store.Store, writer, dlq *kafka.Writer, logger *slog.Logger, options Options) error {
	if err := options.validate(); err != nil {
		return err
	}
	if writer == nil || dlq == nil || writer.Async || dlq.Async || writer.RequiredAcks != kafka.RequireAll || dlq.RequiredAcks != kafka.RequireAll {
		return fmt.Errorf("watcher writers must be synchronous with RequireAll acknowledgements")
	}
	ctx, cancel := context.WithCancel(ctx)
	var background sync.WaitGroup
	background.Add(3)
	go func() {
		defer background.Done()
		sweep(ctx, data, dlq, logger, options)
	}()
	go func() {
		defer background.Done()
		materialize(ctx, data, logger, options)
	}()
	go func() {
		defer background.Done()
		retain(ctx, data, logger, options)
	}()
	defer func() { cancel(); background.Wait() }()
	ticker := time.NewTicker(options.DispatchInterval)
	defer ticker.Stop()
	for {
		for {
			count, err := data.DispatchDue(ctx, options.BatchSize, func(batch []store.DispatchMessage) error {
				messages := make([]kafka.Message, 0, len(batch))
				for _, item := range batch {
					value, err := json.Marshal(item)
					if err != nil {
						return err
					}
					messages = append(messages, kafka.Message{Key: []byte(item.RunID), Value: value})
				}
				publishCtx, stop := context.WithTimeout(ctx, options.OperationTimeout)
				defer stop()
				return writer.WriteMessages(publishCtx, messages...)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("dispatch tick failed", "error", err)
			}
			if count < options.BatchSize || err != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func retain(ctx context.Context, data *store.Store, logger *slog.Logger, options Options) {
	ticker := time.NewTicker(options.RetentionInterval)
	defer ticker.Stop()
	for {
		for {
			requestCtx, stop := context.WithTimeout(ctx, options.OperationTimeout)
			count, err := data.PruneRuns(requestCtx, options.BatchSize)
			stop()
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("retention tick failed", "error", err)
			} else if count > 0 {
				logger.Info("pruned finished runs", "count", count)
			}
			if err != nil || count < int64(options.BatchSize) {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func materialize(ctx context.Context, data *store.Store, logger *slog.Logger, options Options) {
	ticker := time.NewTicker(options.DispatchInterval)
	defer ticker.Stop()
	for {
		requestCtx, stop := context.WithTimeout(ctx, options.OperationTimeout)
		count, err := data.MaterializeCron(requestCtx, options.BatchSize)
		stop()
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("cron materialization tick failed", "error", err)
		} else if count > 0 {
			logger.Debug("materialized cron runs", "count", count)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sweep(ctx context.Context, data *store.Store, dlq *kafka.Writer, logger *slog.Logger, options Options) {
	ticker := time.NewTicker(options.SweepInterval)
	defer ticker.Stop()
	for {
		requestCtx, stop := context.WithTimeout(ctx, options.OperationTimeout)
		result, err := data.Sweep(requestCtx, options.BatchSize, options.QueuedTimeout)
		stop()
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("sweep tick failed", "error", err)
		}
		var letters []queue.DeadLetter
		if err == nil {
			for _, recovered := range result.Recovered {
				logger.Info("recovered expired-worker run", "run_id", recovered.Run.ID, "attempt", recovered.Message.Attempt, "status", recovered.Run.Status)
				if recovered.Run.Status == "DEAD" {
					message := recovered.Message
					letters = append(letters, queue.DeadLetter{Message: &message, Error: recovered.Run.LastError})
				}
			}
			if result.Requeued > 0 {
				logger.Info("requeued stuck Kafka deliveries", "count", result.Requeued)
			}
		}
		if len(letters) > 0 {
			publishCtx, stop := context.WithTimeout(ctx, options.OperationTimeout)
			// ponytail: best-effort after commit; DB DEAD rows remain authoritative.
			err := queue.PublishDeadLetters(publishCtx, dlq, data.Metrics.DLQ, letters...)
			stop()
			if err != nil {
				for _, letter := range letters {
					logger.Error("publish recovered DEAD run to DLQ failed", "run_id", letter.Message.RunID, "attempt", letter.Message.Attempt, "error", err)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
