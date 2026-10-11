package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/api"
)

type httpPayload = api.HTTPPayload

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func permanent(err error) error { return permanentError{err} }

func classify(err error) store.Failure {
	var target permanentError
	failure := store.Failure{Error: err.Error(), Permanent: errors.As(err, &target)}
	var retry retryAfterError
	if errors.As(err, &retry) {
		failure.RetryAfter, failure.RetryAt = retry.after, retry.at
	}
	return failure
}

func Execute(ctx context.Context, client *http.Client, message store.DispatchMessage) error {
	if message.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(message.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch message.Type {
	case "handler":
		var payload struct {
			Handler    string `json:"handler"`
			DurationMS int64  `json:"duration_ms"`
			Error      string `json:"error"`
			Permanent  bool   `json:"permanent"`
		}
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return permanent(fmt.Errorf("decode handler payload: %w", err))
		}
		switch payload.Handler {
		case "noop":
			return ctx.Err()
		case "sleep":
			if payload.DurationMS <= 0 || payload.DurationMS > int64((1<<63-1)/time.Millisecond) {
				return permanent(fmt.Errorf("sleep requires a positive, non-overflowing duration_ms"))
			}
			return wait(ctx, time.Duration(payload.DurationMS)*time.Millisecond)
		case "fail":
			if err := ctx.Err(); err != nil {
				return err
			}
			if payload.Error == "" {
				payload.Error = "handler failure"
			}
			err := errors.New(payload.Error)
			if payload.Permanent {
				return permanent(err)
			}
			return err
		default:
			return permanent(fmt.Errorf("unknown handler %q", payload.Handler))
		}
	case "http":
		var payload httpPayload
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return permanent(fmt.Errorf("decode HTTP payload: %w", err))
		}
		return doHTTP(ctx, client, payload, message.RunID, message.Attempt)
	default:
		return permanent(fmt.Errorf("unknown job type %q", message.Type))
	}
}
