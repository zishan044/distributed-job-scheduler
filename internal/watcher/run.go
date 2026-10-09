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
	DispatchInterval time.Duration
	SweepInterval    time.Duration
	QueuedTimeout    time.Duration
	OperationTimeout time.Duration
	BatchSize        int
}

func DefaultOptions() Options {
	return Options{
		DispatchInterval: 200 * time.Millisecond, SweepInterval: time.Second,
		QueuedTimeout: 5 * time.Minute, OperationTimeout: 5 * time.Second, BatchSize: 1000,
	}
}

func (o Options) validate() error {
	if o.DispatchInterval <= 0 || o.SweepInterval <= 0 || o.QueuedTimeout <= 0 || o.OperationTimeout <= 0 || o.BatchSize < 1 || o.BatchSize > 2000 {
		return fmt.Errorf("invalid watcher batch size or timings")
	}
	return nil
}

// Run keeps sweeping independent of dispatch and its Kafka transaction.
func Run(ctx context.Context, data *store.Store, writer, dlq *kafka.Writer, logger *slog.Logger, options Options) error {
	if err := options.validate(); err != nil {
		return err
	}
	if writer == nil || dlq == nil || writer.Async || dlq.Async || writer.RequiredAcks != kafka.RequireAll || dlq.RequiredAcks != kafka.RequireAll {
		return fmt.Errorf("watcher writers must be synchronous with RequireAll acknowledgements")
	}
	ctx, cancel := context.WithCancel(ctx)
	var sweeping sync.WaitGroup
	sweeping.Add(1)
	go func() {
		defer sweeping.Done()
		sweep(ctx, data, dlq, logger, options)
	}()
	defer func() { cancel(); sweeping.Wait() }()
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
			err := queue.PublishDeadLetters(publishCtx, dlq, letters...)
			stop()
			if err != nil {
				logger.Error("publish recovered DEAD runs to DLQ failed", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
