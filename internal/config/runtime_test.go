package config

import (
	"testing"
	"time"
)

func TestBatchSettings(t *testing.T) {
	for _, setting := range []struct {
		name     string
		read     func() (int, error)
		fallback int
		valid    string
		want     int
		invalid  []string
	}{
		{"WATCHER_BATCH_SIZE", WatcherBatchSize, 1000, " 2000 ", 2000, []string{"0", "-1", "2001", "oops", "1.5"}},
		{"KAFKA_BATCH_SIZE", KafkaBatchSize, 100, " 1000 ", 1000, []string{"0", "-1", "oops", "1.5", "999999999999999999999"}},
		{"EXECUTOR_CONCURRENCY", ExecutorConcurrency, 10, " 1000 ", 1000, []string{"0", "-1", "1001", "oops"}},
	} {
		t.Run(setting.name, func(t *testing.T) {
			for _, empty := range []string{"", " "} {
				t.Setenv(setting.name, empty)
				if got, err := setting.read(); err != nil || got != setting.fallback {
					t.Fatalf("default = %d, %v", got, err)
				}
			}
			t.Setenv(setting.name, setting.valid)
			if got, err := setting.read(); err != nil || got != setting.want {
				t.Fatalf("configured = %d, %v", got, err)
			}
			for _, value := range setting.invalid {
				t.Setenv(setting.name, value)
				if _, err := setting.read(); err == nil {
					t.Fatalf("accepted %q", value)
				}
			}
		})
	}
}

func TestKafkaBatchTimeout(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", 5 * time.Millisecond}, {" ", 5 * time.Millisecond},
		{" 1ms ", time.Millisecond}, {"100us", 100 * time.Microsecond},
	} {
		t.Setenv("KAFKA_BATCH_TIMEOUT", tc.value)
		if got, err := KafkaBatchTimeout(); err != nil || got != tc.want {
			t.Fatalf("%q = %s, %v", tc.value, got, err)
		}
	}
	for _, value := range []string{"0", "0ms", "-1ms", "5", "oops", "999999999999999999999s"} {
		t.Setenv("KAFKA_BATCH_TIMEOUT", value)
		if _, err := KafkaBatchTimeout(); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}
