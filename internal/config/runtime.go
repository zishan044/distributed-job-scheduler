package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func KafkaBrokers() ([]string, error) {
	value := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if value == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS is required")
	}
	var brokers []string
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS is required")
	}
	return brokers, nil
}

func ExecutorConcurrency() (int, error) {
	return envInt("EXECUTOR_CONCURRENCY", 10, 1000)
}

func WatcherBatchSize() (int, error) {
	return envInt("WATCHER_BATCH_SIZE", 1000, 2000)
}

func KafkaBatchSize() (int, error) {
	return envInt("KAFKA_BATCH_SIZE", 100, 0)
}

func KafkaBatchTimeout() (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv("KAFKA_BATCH_TIMEOUT"))
	if value == "" {
		return 5 * time.Millisecond, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("KAFKA_BATCH_TIMEOUT must be a positive duration")
	}
	return timeout, nil
}

func envInt(name string, fallback, maximum int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 || (maximum > 0 && count > maximum) {
		if maximum > 0 {
			return 0, fmt.Errorf("%s must be between 1 and %d", name, maximum)
		}
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return count, nil
}

func HTTPAllowedHosts() []string {
	value := strings.TrimSpace(os.Getenv("HTTP_ALLOWED_HOSTS"))
	if value == "" {
		return nil
	}
	var hosts []string
	for _, host := range strings.Split(value, ",") {
		if host = strings.TrimSpace(host); host != "" {
			hosts = append(hosts, strings.ToLower(host))
		}
	}
	return hosts
}
