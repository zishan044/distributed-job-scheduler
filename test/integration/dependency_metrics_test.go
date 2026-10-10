//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/zishan044/distributed-job-scheduler/internal/metrics"
	"github.com/zishan044/distributed-job-scheduler/internal/queue"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func gaugeValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("missing gauge %s", name)
	return 0
}

func TestActiveWorkerMetrics(t *testing.T) {
	ctx := context.Background()
	local, err := store.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	// Close below also verifies collection against an unavailable pool.
	defer local.Close()
	local.Metrics.RegisterCluster(slog.New(slog.NewTextHandler(io.Discard, nil)), local.ActiveWorkers,
		func(context.Context) (int64, error) { return 0, nil })
	before := gaugeValue(t, local.Metrics.Registry, "active_workers")
	worker := uuid.NewString()
	if err := local.RegisterWorker(ctx, worker, "metrics-test", 2, time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := gaugeValue(t, local.Metrics.Registry, "active_workers"); got != before+1 {
		t.Fatalf("registered worker count = %v, want %v", got, before+1)
	}
	expireWorker(t, worker)
	if got := gaugeValue(t, local.Metrics.Registry, "active_workers"); got != before {
		t.Fatalf("expired worker count = %v, want %v", got, before)
	}
	local.Close()
	if got := gaugeValue(t, local.Metrics.Registry, "active_workers"); !math.IsNaN(got) {
		t.Fatalf("closed database reported %v instead of unknown", got)
	}
}

func TestPoolMetrics(t *testing.T) {
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	observations := metrics.New()
	observations.RegisterPool(pool)
	connection, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()
	for name, want := range map[string]float64{"pgxpool_acquired_conns": 1, "pgxpool_idle_conns": 0, "pgxpool_total_conns": 1, "pgxpool_max_conns": 1} {
		if got := gaugeValue(t, observations.Registry, name); got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	blocked, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if extra, err := pool.Acquire(blocked); err == nil {
		extra.Release()
		t.Fatal("saturated pool should time out")
	}
	families, err := observations.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range families {
		if family.GetName() == "pgxpool_canceled_acquires_total" {
			found = true
			if family.GetMetric()[0].GetCounter().GetValue() != 1 {
				t.Fatal("canceled acquisition was not counted")
			}
		}
	}
	if !found {
		t.Fatal("missing canceled acquisition counter")
	}
	connection.Release()
	if got := gaugeValue(t, observations.Registry, "pgxpool_acquired_conns"); got != 0 {
		t.Fatalf("released connection still acquired: %v", got)
	}
	if got := gaugeValue(t, observations.Registry, "pgxpool_idle_conns"); got != 1 {
		t.Fatalf("released connection not idle: %v", got)
	}
}

func TestKafkaConsumerLag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topic, group := createKafkaTopic(t), "lag-"+uuid.NewString()
	writer := kafkaWriter(topic)
	writer.Balancer = &kafka.RoundRobin{}
	defer writer.Close()
	transport := &kafka.Transport{}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(kafkaBrokers...), Transport: transport}
	assertLag := func(want int64) {
		t.Helper()
		got, err := queue.ConsumerLag(ctx, client, topic, group)
		if err != nil || got != want {
			t.Fatalf("lag = %d, %v, want %d", got, err, want)
		}
	}
	// A fresh broker may still be electing the coordinator for its offsets topic.
	if err := waitFor(ctx, func() error {
		got, err := queue.ConsumerLag(ctx, client, topic, group)
		if err != nil {
			return err
		}
		if got != 0 {
			return fmt.Errorf("empty topic lag = %d", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	messages := make([]kafka.Message, 6)
	for i := range messages {
		messages[i].Value = []byte("lag-test")
	}
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		t.Fatal(err)
	}
	assertLag(6) // Includes all three partitions before the group has any commits.
	reader := kafkaReader(topic, group)
	defer reader.Close()
	for i := range messages {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assertLag(int64(6 - i)) // Fetching alone does not reduce committed lag.
		if err := reader.CommitMessages(ctx, message); err != nil {
			t.Fatal(err)
		}
		assertLag(int64(5 - i))
	}
	// Real network failure respects the caller's deadline and returns an error.
	unavailable := &kafka.Client{Addr: kafka.TCP("127.0.0.1:1")}
	bounded, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	started := time.Now()
	if _, err := queue.ConsumerLag(bounded, unavailable, topic, group); err == nil || time.Since(started) > time.Second {
		t.Fatalf("unavailable broker error=%v, duration=%v", err, time.Since(started))
	}
}
