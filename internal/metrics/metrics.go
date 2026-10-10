// Package metrics defines the scheduler's Prometheus metrics.
package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Registry          *prometheus.Registry
	JobsCreated       prometheus.Counter
	WatcherClaimed    prometheus.Counter
	Executions        *prometheus.CounterVec
	DispatchLatency   prometheus.Histogram
	StartLatency      prometheus.Histogram
	ExecutionDuration *prometheus.HistogramVec
	InflightJobs      prometheus.Gauge
	ActiveWorkers     prometheus.Gauge
	KafkaConsumerLag  prometheus.Gauge
	Retries           prometheus.Counter
	DLQ               prometheus.Counter
	ZombiesRecovered  prometheus.Counter
	StaleCompletions  prometheus.Counter
	RedisErrors       prometheus.Counter
}

// New creates an isolated registry. Callers record observations after successful
// transitions. Cluster gauges are registered only when their collectors are wired.
func New() *Metrics {
	// Include the 2s SLA boundary and enough range for delayed and long-running jobs.
	buckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60, 300}
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		JobsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_created_total", Help: "Jobs newly created by committed transactions, excluding idempotent replays.",
		}),
		WatcherClaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "watcher_claimed_total", Help: "Runs dispatched by committed watcher transactions, including retries and redispatches.",
		}),
		Executions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "executions_total", Help: "Committed execution attempt outcomes.",
		}, []string{"result"}),
		DispatchLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "dispatch_latency_seconds", Help: "Seconds from the current attempt's scheduled time to dispatch.", Buckets: buckets,
		}),
		StartLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "start_latency_seconds", Help: "Seconds from the current attempt's scheduled time to its accepted start.", Buckets: buckets,
		}),
		ExecutionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "execution_duration_seconds", Help: "Elapsed execution time in seconds by job type.", Buckets: buckets,
		}, []string{"type"}),
		InflightJobs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "inflight_jobs", Help: "Jobs currently executing in this process, excluding reserved fetch slots.",
		}),
		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "active_workers", Help: "Workers with live database leases across the cluster.",
		}),
		KafkaConsumerLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag", Help: "Messages in jobs.run awaiting committed offsets for the executors consumer group.",
		}),
		Retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "retries_total", Help: "Committed transitions that schedule a new execution attempt, including zombie recovery.",
		}),
		DLQ: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dlq_total", Help: "Acknowledged dead-letter publications, including malformed messages; not a count of DEAD runs.",
		}),
		ZombiesRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "zombies_recovered_total", Help: "Expired-worker runs recovered by committed sweeper transactions.",
		}),
		StaleCompletions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stale_completions_total", Help: "Execution result writes rejected by fencing, excluding database errors.",
		}),
		RedisErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "redis_errors_total", Help: "Failed Redis operations, excluding cache misses.",
		}),
	}
	for _, result := range []string{"succeeded", "failed", "dead", "cancelled"} {
		m.Executions.WithLabelValues(result)
	}
	for _, jobType := range []string{"http", "handler"} {
		m.ExecutionDuration.WithLabelValues(jobType)
	}
	m.Registry.MustRegister(
		m.JobsCreated, m.WatcherClaimed, m.Executions,
		m.DispatchLatency, m.StartLatency, m.ExecutionDuration, m.InflightJobs,
		m.Retries, m.DLQ, m.ZombiesRecovered, m.StaleCompletions, m.RedisErrors,
	)
	return m
}
