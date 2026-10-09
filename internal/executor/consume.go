package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

const (
	claimWindow   = 5 * time.Millisecond
	resultTimeout = 5 * time.Second
)

// DeadLetter contains either a failed dispatch or the source of a malformed message.
type DeadLetter struct {
	Message *store.DispatchMessage `json:"message,omitempty"`
	Error   string                 `json:"error"`
	Source  *DeadLetterSource      `json:"source,omitempty"`
}

type DeadLetterSource struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       []byte `json:"key"`
	RawValue  []byte `json:"raw_value"` // Base64 preserves malformed JSON and invalid UTF-8.
}

// Consume runs the bounded worker pool. The caller owns the concrete dependencies.
func Consume(ctx context.Context, reader *kafka.Reader, data *store.Store, client *http.Client, dlq *kafka.Writer, workerID string, concurrency int, logger *slog.Logger) error {
	if concurrency < 1 {
		return fmt.Errorf("executor concurrency must be positive")
	}
	if id, err := uuid.Parse(workerID); err != nil || id == uuid.Nil {
		return fmt.Errorf("invalid worker ID")
	}
	if dlq == nil || dlq.Async || dlq.RequiredAcks != kafka.RequireAll {
		return fmt.Errorf("DLQ writer must be synchronous with RequireAll acknowledgements")
	}
	ctx, cancel := context.WithCancel(ctx)
	var running sync.WaitGroup
	defer func() {
		cancel()
		running.Wait()
	}()
	active := make(chan struct{}, concurrency)
	for {
		select {
		case active <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		first, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		messages := []kafka.Message{first}
	collecting:
		for len(messages) < concurrency {
			select {
			case active <- struct{}{}:
			default:
				break collecting
			}
			fetchCtx, stop := context.WithTimeout(ctx, claimWindow)
			message, err := reader.FetchMessage(fetchCtx)
			stop()
			if err != nil {
				<-active
				if !errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				break collecting
			}
			messages = append(messages, message)
		}

		dispatches := make([]store.DispatchMessage, 0, len(messages))
		for _, message := range messages {
			dispatch, err := decode(message.Value)
			if err == nil {
				dispatches = append(dispatches, dispatch)
				continue
			}
			publishCtx, stop := context.WithTimeout(ctx, resultTimeout)
			publishErr := writeDeadLetter(publishCtx, dlq, message.Key, DeadLetter{
				Error: err.Error(),
				Source: &DeadLetterSource{
					Topic: message.Topic, Partition: message.Partition, Offset: message.Offset,
					Key: message.Key, RawValue: message.Value,
				},
			})
			stop()
			if publishErr != nil {
				return fmt.Errorf("publish malformed message to DLQ: %w", publishErr)
			}
			<-active
		}
		var claimed []store.DispatchMessage
		if len(dispatches) > 0 {
			claimed, err = data.StartRuns(ctx, dispatches, workerID)
			if err != nil {
				return err
			}
		}
		if err := reader.CommitMessages(ctx, messages...); err != nil {
			logger.Error("commit Kafka messages failed", "error", err)
		}
		for range len(dispatches) - len(claimed) {
			<-active
		}
		for _, item := range claimed {
			running.Add(1)
			go func() {
				defer running.Done()
				defer func() { <-active }()
				executionErr := Execute(ctx, client, item)
				// A cancelled fetch context must not prevent recording the result.
				resultCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), resultTimeout)
				defer stop()
				if executionErr == nil {
					applied, err := data.SucceedRun(resultCtx, item.RunID, item.Attempt, workerID)
					logResult(logger, item, applied, err)
					return
				}
				logger.Warn("job execution failed", "run_id", item.RunID, "attempt", item.Attempt, "error", executionErr)
				run, err := data.FailRun(resultCtx, item.RunID, item.Attempt, workerID, classify(executionErr))
				logResult(logger, item, run.ID != "", err)
				if err == nil && run.Status == "DEAD" {
					// ponytail: best-effort after commit; add durable delivery if required.
					if err := writeDeadLetter(resultCtx, dlq, []byte(item.RunID), DeadLetter{Message: &item, Error: executionErr.Error()}); err != nil {
						logger.Error("publish DEAD run to DLQ failed", "run_id", item.RunID, "attempt", item.Attempt, "error", err)
					}
				}
			}()
		}
	}
}

func logResult(logger *slog.Logger, item store.DispatchMessage, applied bool, err error) {
	if err != nil {
		logger.Error("save job result failed", "run_id", item.RunID, "attempt", item.Attempt, "error", err)
	} else if !applied {
		logger.Warn("discarded stale job completion", "run_id", item.RunID, "attempt", item.Attempt)
	}
}

func writeDeadLetter(ctx context.Context, writer *kafka.Writer, key []byte, letter DeadLetter) error {
	value, err := json.Marshal(letter)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

func decode(value []byte) (store.DispatchMessage, error) {
	var message store.DispatchMessage
	if err := json.Unmarshal(value, &message); err != nil {
		return message, fmt.Errorf("decode dispatch: %w", err)
	}
	for _, id := range []string{message.RunID, message.JobID} {
		if parsed, err := uuid.Parse(id); err != nil || parsed == uuid.Nil {
			return message, fmt.Errorf("dispatch requires valid nonzero run_id and job_id")
		}
	}
	if message.Attempt < 1 || message.Attempt > 1<<31-1 || message.TimeoutMS < 1 || message.TimeoutMS > 1<<31-1 || message.FireTime.IsZero() {
		return message, fmt.Errorf("dispatch requires positive attempt and timeout_ms and a fire_time")
	}
	if (message.Type != "handler" && message.Type != "http") || !json.Valid(message.Payload) {
		return message, fmt.Errorf("dispatch requires a supported type and JSON payload")
	}
	return message, nil
}
