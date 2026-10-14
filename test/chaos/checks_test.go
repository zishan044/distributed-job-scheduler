//go:build chaos

package chaos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestContainerScope(t *testing.T) {
	item := container{ID: "executor-id"}
	item.Config.Labels = map[string]string{
		"com.docker.compose.project": projectName, "com.docker.compose.service": "executor",
	}
	if err := item.validate("executor"); err != nil {
		t.Fatal(err)
	}
	if err := item.validate("watcher"); err == nil {
		t.Fatal("accepted the wrong service")
	}
	item.Config.Labels["com.docker.compose.project"] = "development"
	if err := item.validate("executor"); err == nil {
		t.Fatal("accepted the development project")
	}
	item.Config.Labels = nil
	if err := item.validate("executor"); err == nil {
		t.Fatal("accepted a container without labels")
	}
}

func TestTargetHoldRelease(t *testing.T) {
	f := new(targetFixture)
	release := f.hold("/held", 1)
	defer release()
	server := httptest.NewServer(f)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/held", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Idempotency-Key", "run-id")
	request.Header.Set("X-Job-Attempt", "1")
	done := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	if err := waitFor(ctx, func(context.Context) error {
		if len(f.calls("/held")) == 0 {
			return context.DeadlineExceeded
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	calls := f.calls("/held")
	if len(calls) != 1 || calls[0].FinishedAt != nil || calls[0].IdempotencyKey != "run-id" || calls[0].Attempt != 1 {
		t.Fatalf("held call = %+v", calls)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	calls = f.calls("/held")
	if calls[0].FinishedAt == nil || calls[0].Cancelled {
		t.Fatalf("released call = %+v", calls)
	}
}
