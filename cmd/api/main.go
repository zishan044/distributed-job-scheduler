package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/zishan044/distributed-job-scheduler/internal/config"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/logging"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func main() {
	logger := logging.New("api")
	logger.Info("starting service")
	if err := run(logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL, err := config.DatabaseURL()
	if err != nil {
		return err
	}
	apiKey, err := config.APIKey()
	if err != nil {
		return err
	}

	data, err := store.New(context.Background(), databaseURL)
	if err != nil {
		return err
	}
	defer data.Close()

	cache := redis.NewClient(&redis.Options{Addr: config.RedisAddr()})
	defer cache.Close()

	return httpserver.Serve(config.HTTPAddr(), httpserver.NewAPIRouter(logger, data, cache, apiKey), logger)
}
