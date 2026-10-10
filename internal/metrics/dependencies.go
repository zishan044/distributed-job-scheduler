package metrics

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterCluster is called only by the API. Unknown values are NaN so a failed
// dependency does not look healthy or prevent scraping the remaining metrics.
func (m *Metrics) RegisterCluster(logger *slog.Logger, activeWorkers, consumerLag func(context.Context) (int64, error)) {
	for _, source := range []struct {
		name, help string
		read       func(context.Context) (int64, error)
	}{
		{"active_workers", "Workers with live database leases across the cluster; NaN when collection fails.", activeWorkers},
		{"kafka_consumer_lag", "Retained jobs.run messages awaiting committed offsets for the executors group; NaN when collection fails.", consumerLag},
	} {
		m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: source.name, Help: source.help}, func() float64 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			value, err := source.read(ctx)
			if err != nil {
				logger.Warn("collect cluster metric", "metric", source.name, "error", err)
				return math.NaN()
			}
			return float64(value)
		}))
	}
}

// RegisterPool reads pgx's local statistics without querying Postgres.
func (m *Metrics) RegisterPool(pool *pgxpool.Pool) {
	for _, stat := range []struct {
		name, help string
		read       func(*pgxpool.Stat) float64
	}{
		{"pgxpool_acquired_conns", "Currently acquired pool connections.", func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }},
		{"pgxpool_idle_conns", "Currently idle pool connections.", func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) }},
		{"pgxpool_total_conns", "Total acquired, idle and constructing pool connections.", func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) }},
		{"pgxpool_max_conns", "Configured maximum pool connections.", func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }},
		{"pgxpool_constructing_conns", "Pool connections being constructed.", func(s *pgxpool.Stat) float64 { return float64(s.ConstructingConns()) }},
	} {
		m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: stat.name, Help: stat.help}, func() float64 {
			return stat.read(pool.Stat())
		}))
	}
	for _, stat := range []struct {
		name, help string
		read       func(*pgxpool.Stat) float64
	}{
		{"pgxpool_acquires_total", "Successful pool acquisitions.", func(s *pgxpool.Stat) float64 { return float64(s.AcquireCount()) }},
		{"pgxpool_acquire_duration_seconds_total", "Cumulative time spent in successful pool acquisitions.", func(s *pgxpool.Stat) float64 { return s.AcquireDuration().Seconds() }},
		{"pgxpool_canceled_acquires_total", "Pool acquisitions canceled by context.", func(s *pgxpool.Stat) float64 { return float64(s.CanceledAcquireCount()) }},
		{"pgxpool_empty_acquires_total", "Successful acquisitions that waited because the pool was empty.", func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) }},
		{"pgxpool_empty_acquire_wait_seconds_total", "Cumulative wait time for successful acquisitions from an empty pool.", func(s *pgxpool.Stat) float64 { return s.EmptyAcquireWaitTime().Seconds() }},
	} {
		m.Registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: stat.name, Help: stat.help}, func() float64 {
			return stat.read(pool.Stat())
		}))
	}
}
