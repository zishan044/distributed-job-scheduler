package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/queue"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

const (
	claimWindow   = 5 * time.Millisecond
	resultTimeout = 5 * time.Second
)

// Consume owns worker identities and the bounded pool; callers own dependencies.
func Consume(ctx context.Context, reader *kafka.Reader, data *store.Store, client *http.Client, dlq *kafka.Writer, logger *slog.Logger, options Options) error {
	if err := options.validate(); err != nil {
		return err
	}
	if dlq == nil || dlq.Async || dlq.RequiredAcks != kafka.RequireAll {
		return fmt.Errorf("DLQ writer must be synchronous with RequireAll acknowledgements")
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	leaseCtx, stopLeases := context.WithCancel(context.WithoutCancel(ctx))
	executionCtx, stopExecution := context.WithCancelCause(leaseCtx)
	var running, heartbeats sync.WaitGroup
	// Fetch/claim cancellation starts the drain; leases stay alive until all
	// bounded result writes finish. Only the drain deadline interrupts execution.
	defer func() {
		cancel()
		done := make(chan struct{})
		go func() { running.Wait(); close(done) }()
		timer := time.NewTimer(options.DrainTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			logger.Warn("shutdown drain expired; cancelling remaining jobs")
			stopExecution(errDrainExpired)
			<-done
		}
		stopExecution(context.Canceled)
		stopLeases()
		heartbeats.Wait()
	}()
	active := make(chan struct{}, options.Concurrency)
	var session *workerSession
	intakeCtx := ctx
	stopIntake := func() {}
	stopFence := func() bool { return false }
	defer func() { stopFence(); stopIntake() }()
	// Hold fetched messages and their reserved slots until the batch is claimed.
	// Re-registering a worker must not advance past an unclaimed Kafka message.
	var pending []kafka.Message
consuming:
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if session == nil || session.ctx.Err() != nil {
			session, err = registerSession(ctx, leaseCtx, data, hostname, options)
			if err != nil {
				logger.Warn("register worker failed", "error", err)
				if err := wait(ctx, options.HeartbeatInterval); err != nil {
					return err
				}
				continue
			}
			stopFence()
			stopIntake()
			intakeCtx, stopIntake = context.WithCancel(ctx)
			stopFence = context.AfterFunc(session.ctx, stopIntake)
			logger.Info("registered worker", "worker_id", session.id, "capacity", options.Concurrency)
			owner := session
			heartbeats.Add(1)
			go func() { defer heartbeats.Done(); owner.keepLease(data, options, logger) }()
		}
		if len(pending) == 0 {
			select {
			case active <- struct{}{}:
			case <-intakeCtx.Done():
				continue
			}
			first, err := reader.FetchMessage(intakeCtx)
			if err != nil {
				<-active
				if errors.Is(context.Cause(session.ctx), store.ErrLeaseLost) {
					continue
				}
				return err
			}
			pending = []kafka.Message{first}
		collecting:
			for len(pending) < options.Concurrency {
				select {
				case <-intakeCtx.Done():
					break collecting
				case active <- struct{}{}:
				default:
					break collecting
				}
				fetchCtx, stop := context.WithTimeout(intakeCtx, claimWindow)
				message, err := reader.FetchMessage(fetchCtx)
				stop()
				if err != nil {
					<-active
					if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(context.Cause(session.ctx), store.ErrLeaseLost) {
						return err
					}
					break collecting
				}
				pending = append(pending, message)
			}
		}
		if intakeCtx.Err() != nil {
			continue
		}
		dispatches := make([]store.DispatchMessage, 0, len(pending))
		for _, message := range pending {
			dispatch, err := decode(message.Value)
			if err == nil {
				dispatches = append(dispatches, dispatch)
				continue
			}
			publishCtx, stop := context.WithTimeout(intakeCtx, resultTimeout)
			publishErr := queue.PublishDeadLetters(publishCtx, dlq, data.Metrics.DLQ, queue.DeadLetter{
				Error:  err.Error(),
				Source: &queue.DeadLetterSource{Topic: message.Topic, Partition: message.Partition, Offset: message.Offset, Key: message.Key, RawValue: message.Value},
			})
			stop()
			if publishErr != nil {
				if errors.Is(context.Cause(session.ctx), store.ErrLeaseLost) {
					continue consuming
				}
				return fmt.Errorf("publish malformed message to DLQ: %w", publishErr)
			}
		}
		var claimed []store.DispatchMessage
		if len(dispatches) > 0 {
			claimed, err = data.StartRuns(intakeCtx, dispatches, session.id)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// A failed commit may have persisted claims. Abandon this worker
				// so the sweeper can recover them instead of renewing it forever.
				logger.Warn("start claim failed", "worker_id", session.id, "error", err)
				session.fence(logger)
				continue
			}
		}
		if err := reader.CommitMessages(intakeCtx, pending...); err != nil {
			logger.Error("commit Kafka messages failed", "error", err)
		}
		for range len(pending) - len(claimed) {
			<-active
		}
		pending = nil
		owner := session
		for _, item := range claimed {
			key := attemptKey{id: item.RunID, attempt: item.Attempt}
			jobCtx := owner.beginRun(executionCtx, key)
			running.Add(1)
			go func() {
				defer running.Done()
				defer func() { <-active }()
				defer owner.endRun(key)
				if errors.Is(context.Cause(owner.ctx), store.ErrLeaseLost) {
					return
				}
				runLogger := logger.With("run_id", item.RunID, "attempt", item.Attempt, "worker_id", owner.id)
				runLogger.Debug("starting job execution")
				started := time.Now()
				data.Metrics.InflightJobs.Inc()
				executionErr := jobCtx.Err()
				if executionErr == nil {
					executionErr = execute(jobCtx, client, item, options.Handlers)
				}
				data.Metrics.ExecutionDuration.WithLabelValues(item.Type).Observe(time.Since(started).Seconds())
				data.Metrics.InflightJobs.Dec()
				if errors.Is(context.Cause(owner.ctx), store.ErrLeaseLost) {
					return
				}
				if cause := context.Cause(jobCtx); cause != nil {
					executionErr = cause
				}
				resultCtx, stop := context.WithTimeout(context.WithoutCancel(jobCtx), resultTimeout)
				defer stop()
				stopFence := context.AfterFunc(owner.ctx, func() {
					if errors.Is(context.Cause(owner.ctx), store.ErrLeaseLost) {
						stop()
					}
				})
				defer stopFence()
				if executionErr == nil {
					applied, err := data.SucceedRun(resultCtx, item.RunID, item.Attempt, owner.id)
					logResult(runLogger, applied, err)
					return
				}
				runLogger.Warn("job execution failed", "error", executionErr)
				run, err := data.FailRun(resultCtx, item.RunID, item.Attempt, owner.id, classify(executionErr))
				logResult(runLogger, run.ID != "", err)
				if err == nil {
					switch run.Status {
					case "PENDING":
						runLogger.Info("scheduled retry", "next_attempt", run.Attempt, "scheduled_for", run.ScheduledFor)
					case "CANCELLED":
						runLogger.Info("run cancelled")
					}
				}
				if err == nil && run.Status == "DEAD" {
					// ponytail: best-effort after commit; add durable delivery if required.
					if err := queue.PublishDeadLetters(resultCtx, dlq, data.Metrics.DLQ, queue.DeadLetter{Message: &item, Error: executionErr.Error()}); err != nil {
						runLogger.Error("publish DEAD run to DLQ failed", "error", err)
					}
				}
			}()
		}
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func logResult(logger *slog.Logger, applied bool, err error) {
	if err != nil {
		logger.Error("save job result failed", "error", err)
	} else if !applied {
		logger.Warn("discarded stale job completion")
	} else {
		logger.Debug("saved job result")
	}
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
