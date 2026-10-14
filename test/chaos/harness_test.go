//go:build chaos

package chaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
	"github.com/zishan044/distributed-job-scheduler/pkg/client"
)

const projectName = "scheduler-chaos"

var (
	suiteCtx context.Context
	target   = new(targetFixture)
)

func TestMain(m *testing.M) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	suiteCtx = ctx
	defer cancel()
	if os.Getenv("DATABASE_URL") == "" {
		os.Exit(m.Run()) // Pure fixture/label checks can run without Docker.
	}
	listener, err := net.Listen("tcp", ":8090")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	server := &http.Server{Handler: target, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	status := m.Run()
	_ = server.Close()
	cancel()
	os.Exit(status)
}

type container struct {
	ID     string `json:"Id"`
	Config struct {
		Hostname string            `json:"Hostname"`
		Labels   map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool `json:"Running"`
		Paused  bool `json:"Paused"`
	} `json:"State"`
	HostConfig struct {
		RestartPolicy struct {
			Name              string `json:"Name"`
			MaximumRetryCount int    `json:"MaximumRetryCount"`
		} `json:"RestartPolicy"`
	} `json:"HostConfig"`
	RestartCount    int `json:"RestartCount"`
	NetworkSettings struct {
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

func (c container) validate(service string) error {
	if c.ID == "" || c.Config.Labels["com.docker.compose.project"] != projectName ||
		c.Config.Labels["com.docker.compose.service"] != service {
		return fmt.Errorf("refusing container %q outside %s/%s", c.ID, projectName, service)
	}
	return nil
}

func docker(ctx context.Context, args ...string) ([]byte, error) {
	commandCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, "docker", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker %v: %w: %s", args, err, output)
	}
	return output, nil
}

func inspect(ctx context.Context, id, service string) (container, error) {
	var items []container
	output, err := docker(ctx, "inspect", id)
	if err == nil {
		err = json.Unmarshal(output, &items)
	}
	if err != nil {
		return container{}, err
	}
	if len(items) != 1 || items[0].ID != id {
		return container{}, fmt.Errorf("unexpected inspection result for %s", id)
	}
	return items[0], items[0].validate(service)
}

func containers(ctx context.Context, service string) ([]container, error) {
	output, err := docker(ctx, "ps", "-a", "--no-trunc", "-q", "--filter", "label=com.docker.compose.project="+projectName,
		"--filter", "label=com.docker.compose.service="+service)
	if err != nil {
		return nil, err
	}
	items := make([]container, 0)
	for _, id := range strings.Fields(string(output)) {
		item, err := inspect(ctx, id, service)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func waitFor(ctx context.Context, check func(context.Context) error) error {
	var last error
	for {
		requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		last = check(requestCtx)
		cancel()
		if last == nil {
			return nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w; last observation: %v", ctx.Err(), last)
		case <-timer.C:
		}
	}
}

type harness struct {
	t         *testing.T
	data      *store.Store
	client    *client.Client
	directory string
	path      string
	runID     string
	executors []container
	watchers  []container
}

func newHarness(t *testing.T, ctx context.Context) *harness {
	t.Helper()
	if os.Getenv("DATABASE_URL") == "" {
		t.Fatal("use make chaos to run container scenarios")
	}
	h := &harness{t: t, path: "/" + uuid.NewString()}
	h.directory = filepath.Join("/results", t.Name()+"-"+uuid.NewString())
	if err := os.MkdirAll(h.directory, 0755); err != nil {
		t.Fatal(err)
	}
	var err error
	h.client, err = client.New("http://api:8080", client.WithAPIKey(os.Getenv("API_KEY")), client.WithRetry(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := waitFor(ctx, func(ctx context.Context) error {
		h.data, err = store.New(ctx, os.Getenv("DATABASE_URL"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.data.Close)
	t.Cleanup(h.snapshot)
	if err := waitFor(ctx, func(ctx context.Context) error {
		_, err := h.client.GetJob(ctx, uuid.NewString())
		var response *client.HTTPError
		if !errors.As(err, &response) || response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("API database read: %v", err)
		}
		count, err := h.data.ActiveWorkers(ctx)
		if err != nil || count != 3 {
			return fmt.Errorf("live workers = %d: %v", count, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h.executors, err = containers(ctx, "executor")
	if err != nil || len(h.executors) != 3 {
		t.Fatalf("executor discovery: %d containers, %v", len(h.executors), err)
	}
	h.watchers, err = containers(ctx, "watcher")
	if err != nil || len(h.watchers) != 2 {
		t.Fatalf("watcher discovery: %d containers, %v", len(h.watchers), err)
	}
	h.event("ready", map[string]any{"executors": h.executors, "watchers": h.watchers})
	return h
}

func (h *harness) event(action string, detail any) {
	h.t.Helper()
	file, err := os.OpenFile(filepath.Join(h.directory, "timeline.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		h.t.Error(err)
		return
	}
	defer file.Close()
	err = json.NewEncoder(file).Encode(map[string]any{"at": time.Now().UTC(), "action": action, "detail": detail})
	if err != nil {
		h.t.Error(err)
	}
	h.t.Log(action)
}

func (h *harness) pause(ctx context.Context, item container) {
	h.t.Helper()
	service := item.Config.Labels["com.docker.compose.service"]
	if _, err := inspect(ctx, item.ID, service); err != nil {
		h.t.Fatal(err)
	}
	// Register before mutation: a lost Docker response may still have paused it.
	h.t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		current, err := inspect(cleanupCtx, item.ID, service)
		if err == nil && current.State.Paused {
			_, err = docker(cleanupCtx, "unpause", item.ID)
		}
		if err != nil {
			h.t.Errorf("restore %s: %v", item.ID, err)
		}
	})
	if _, err := docker(ctx, "pause", item.ID); err != nil {
		h.t.Fatal(err)
	}
	h.event("paused", item.ID)
}

func (h *harness) resume(ctx context.Context, item container) {
	h.t.Helper()
	if _, err := inspect(ctx, item.ID, "executor"); err != nil {
		h.t.Fatal(err)
	}
	if _, err := docker(ctx, "unpause", item.ID); err != nil {
		h.t.Fatal(err)
	}
	h.event("unpaused", item.ID)
}

func (h *harness) write(name string, value any) {
	h.t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(h.directory, name), append(encoded, '\n'), 0644)
	}
	if err != nil {
		h.t.Error(err)
	}
}

func (h *harness) snapshot() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.write("http.json", target.calls(h.path))
	workers, err := h.data.ChaosWorkers(ctx)
	if err != nil {
		h.t.Error(err)
	} else {
		h.write("workers.json", workers)
	}
	if h.runID != "" {
		run, err := h.data.GetRun(ctx, h.runID)
		if err != nil {
			h.t.Error(err)
		}
		attempts, err := h.data.ChaosAttempts(ctx, h.runID)
		if err != nil {
			h.t.Error(err)
		}
		h.write("database.json", map[string]any{"run": run, "attempts": attempts})
	}
	for _, service := range []string{"api", "watcher", "executor"} {
		items, err := containers(ctx, service)
		if err != nil {
			h.t.Error(err)
			continue
		}
		h.write(service+"-containers.json", items)
		for _, item := range items {
			network, ok := item.NetworkSettings.Networks[projectName+"_default"]
			if !ok || net.ParseIP(network.IPAddress) == nil {
				h.t.Errorf("missing chaos network IP for %s", item.ID)
				continue
			}
			requestCtx, stop := context.WithTimeout(ctx, 3*time.Second)
			request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, "http://"+net.JoinHostPort(network.IPAddress, "8080")+"/metrics", nil)
			if err == nil {
				var response *http.Response
				response, err = http.DefaultClient.Do(request)
				if err == nil {
					var body []byte
					body, err = io.ReadAll(io.LimitReader(response.Body, 2<<20))
					_ = response.Body.Close()
					if err == nil && response.StatusCode != http.StatusOK {
						err = fmt.Errorf("metrics HTTP %d", response.StatusCode)
					}
					if err == nil {
						err = os.WriteFile(filepath.Join(h.directory, service+"-"+item.Config.Hostname+"-metrics.txt"), body, 0644)
					}
				}
			}
			stop()
			if err != nil {
				h.t.Error(err)
			}
		}
	}
}
