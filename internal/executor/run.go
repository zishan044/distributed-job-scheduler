package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

type httpPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

func Execute(ctx context.Context, client *http.Client, message store.DispatchMessage) error {
	if message.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(message.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	switch message.Type {
	case "handler":
		var payload struct {
			Handler string `json:"handler"`
		}
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return fmt.Errorf("decode handler payload: %w", err)
		}
		if payload.Handler != "noop" {
			return fmt.Errorf("unknown handler %q", payload.Handler)
		}
		return nil
	case "http":
		var payload httpPayload
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return fmt.Errorf("decode HTTP payload: %w", err)
		}
		return doHTTP(ctx, client, payload)
	default:
		return fmt.Errorf("unknown job type %q", message.Type)
	}
}
