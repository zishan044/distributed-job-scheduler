package httpserver

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeContextLifecycle(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	// A bind failure must return without a serving goroutine or signal handler.
	if err := ServeContext(context.Background(), addr, http.NotFoundHandler(), logger); err == nil {
		t.Fatal("accepted occupied address")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- ServeContext(ctx, addr, http.NotFoundHandler(), logger) }()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get("http://" + addr)
		if err == nil {
			_ = response.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop")
	}
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener leaked: %v", err)
	}
	_ = listener.Close()
	if err := ServeContext(ctx, addr, http.NotFoundHandler(), logger); err != nil {
		t.Fatal(err)
	}
}
