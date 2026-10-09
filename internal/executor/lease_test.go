package executor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func TestLeaseOptions(t *testing.T) {
	valid := DefaultOptions(10)
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Options){
		func(o *Options) { o.Concurrency = 0 },
		func(o *Options) { o.Concurrency = 1001 },
		func(o *Options) { o.HeartbeatInterval = 0 },
		func(o *Options) { o.LeaseDuration = 0 },
		func(o *Options) { o.SafetyMargin = 0 },
		func(o *Options) { o.SafetyMargin = o.LeaseDuration },
		func(o *Options) { o.HeartbeatInterval = o.LeaseDuration - o.SafetyMargin },
	} {
		o := valid
		change(&o)
		if err := o.validate(); err == nil {
			t.Fatalf("accepted invalid options: %+v", o)
		}
	}
}

func TestSelfFenceCancelsEveryOwnedAttempt(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	session := &workerSession{id: "original-worker", ctx: ctx, cancel: cancel, active: make(map[attemptKey]context.CancelCauseFunc)}
	firstKey, secondKey := attemptKey{id: "run", attempt: 1}, attemptKey{id: "run", attempt: 2}
	first := session.beginRun(context.Background(), firstKey)
	second := session.beginRun(context.Background(), secondKey)
	defer session.endRun(firstKey)
	defer session.endRun(secondKey)
	session.fence(slog.New(slog.NewTextHandler(io.Discard, nil)))
	lateKey := attemptKey{id: "late", attempt: 1}
	late := session.beginRun(context.Background(), lateKey)
	defer session.endRun(lateKey)
	for _, job := range []context.Context{first, second, late} {
		select {
		case <-job.Done():
		case <-time.After(time.Second):
			t.Fatal("owned attempt was not cancelled")
		}
		if !errors.Is(context.Cause(job), store.ErrLeaseLost) {
			t.Fatalf("cancel cause = %v", context.Cause(job))
		}
	}
}
