package main

import (
	"os"

	"github.com/zishan044/distributed-job-scheduler/internal/config"
	"github.com/zishan044/distributed-job-scheduler/internal/httpserver"
	"github.com/zishan044/distributed-job-scheduler/internal/logging"
)

func main() {
	logger := logging.New("executor")
	logger.Info("starting service")
	if err := httpserver.Serve(config.HTTPAddr(), httpserver.NewRouter("executor", logger), logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
