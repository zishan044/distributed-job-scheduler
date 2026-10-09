package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

type Options struct {
	Concurrency       int
	HeartbeatInterval time.Duration
	LeaseDuration     time.Duration
	SafetyMargin      time.Duration
	DrainTimeout      time.Duration
}

func DefaultOptions(concurrency int) Options {
	return Options{Concurrency: concurrency, HeartbeatInterval: 5 * time.Second, LeaseDuration: 15 * time.Second, SafetyMargin: time.Second, DrainTimeout: 30 * time.Second}
}

func (o Options) validate() error {
	if o.Concurrency < 1 || o.Concurrency > 1000 || o.HeartbeatInterval <= 0 || o.SafetyMargin <= 0 ||
		o.LeaseDuration <= o.SafetyMargin || o.HeartbeatInterval >= o.LeaseDuration-o.SafetyMargin || o.DrainTimeout <= 0 {
		return fmt.Errorf("invalid executor capacity or lease timings")
	}
	return nil
}

var (
	errRunCancelled = errors.New("job cancellation requested")
	errDrainExpired = errors.New("executor shutdown drain expired")
)

type attemptKey struct {
	id      string
	attempt int
}

type workerSession struct {
	id       string
	ctx      context.Context
	cancel   context.CancelCauseFunc
	deadline time.Time
	mu       sync.Mutex
	active   map[attemptKey]context.CancelCauseFunc
}

func registerSession(ctx, leaseCtx context.Context, data *store.Store, hostname string, options Options) (*workerSession, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	sent := time.Now()
	requestCtx, stop := context.WithTimeout(ctx, min(resultTimeout, options.LeaseDuration-options.SafetyMargin))
	err = data.RegisterWorker(requestCtx, id.String(), hostname, options.Concurrency, options.LeaseDuration)
	stop()
	if err != nil {
		return nil, err
	}
	deadline := sent.Add(options.LeaseDuration - options.SafetyMargin)
	if !time.Now().Before(deadline) {
		return nil, store.ErrLeaseLost
	}
	sessionCtx, cancel := context.WithCancelCause(leaseCtx)
	return &workerSession{id: id.String(), ctx: sessionCtx, cancel: cancel, deadline: deadline, active: make(map[attemptKey]context.CancelCauseFunc)}, nil
}

func (s *workerSession) cancelRuns(items []store.Cancellation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range items {
		if cancel, ok := s.active[attemptKey{id: item.RunID, attempt: item.Attempt}]; ok {
			cancel(errRunCancelled)
		}
	}
}

func (s *workerSession) beginRun(ctx context.Context, key attemptKey) context.Context {
	jobCtx, cancel := context.WithCancelCause(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		cancel(context.Cause(s.ctx))
	}
	s.active[key] = cancel
	return jobCtx
}

func (s *workerSession) endRun(key attemptKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[key](context.Canceled)
	delete(s.active, key)
}

func (s *workerSession) fence(logger *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return
	}
	s.cancel(store.ErrLeaseLost)
	for _, cancel := range s.active {
		cancel(store.ErrLeaseLost)
	}
	logger.Warn("worker self-fenced", "worker_id", s.id, "inflight", len(s.active))
}

// The timer runs independently of the DB request, so a stalled heartbeat cannot
// keep handlers alive past the local monotonic deadline.
func (s *workerSession) keepLease(data *store.Store, options Options, logger *slog.Logger) {
	deadline := s.deadline
	timer := time.NewTimer(time.Until(deadline))
	ticker := time.NewTicker(options.HeartbeatInterval)
	defer timer.Stop()
	defer ticker.Stop()
	type heartbeatResult struct {
		sent      time.Time
		heartbeat store.Heartbeat
		err       error
	}
	results := make(chan heartbeatResult, 1)
	var requests sync.WaitGroup
	defer requests.Wait()
	renewing := false
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			s.fence(logger)
			return
		case <-ticker.C:
			if renewing {
				continue
			}
			sent := time.Now()
			if !sent.Before(deadline) {
				s.fence(logger)
				return
			}
			renewing = true
			requestCtx, stop := context.WithDeadline(s.ctx, deadline)
			requests.Add(1)
			go func() {
				defer requests.Done()
				defer stop()
				heartbeat, err := data.HeartbeatWorker(requestCtx, s.id, options.LeaseDuration)
				select {
				case results <- heartbeatResult{sent: sent, heartbeat: heartbeat, err: err}:
				case <-s.ctx.Done():
				}
			}()
		case result := <-results:
			renewing = false
			if !time.Now().Before(deadline) {
				s.fence(logger)
				return
			}
			if result.err != nil {
				logger.Warn("worker heartbeat failed", "worker_id", s.id, "error", result.err)
				continue
			}
			if !result.heartbeat.Renewed {
				s.fence(logger)
				return
			}
			deadline = result.sent.Add(options.LeaseDuration - options.SafetyMargin)
			timer.Reset(time.Until(deadline))
			s.cancelRuns(result.heartbeat.Cancellations)
		}
	}
}
