// Command http-reminder schedules a one-shot HTTP call and waits for success.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zishan044/distributed-job-scheduler/pkg/api"
	"github.com/zishan044/distributed-job-scheduler/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	baseURL := flag.String("api-url", "http://localhost:8080", "scheduler API URL")
	targetURL := flag.String("target-url", "http://echo:5678/", "URL called by the executor")
	flag.Parse()
	key := os.Getenv("API_KEY")
	if key == "" {
		key = "scheduler-local"
	}
	c, err := client.New(*baseURL, client.WithAPIKey(key))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	when := time.Now().UTC()
	job, err := c.ScheduleHTTP(ctx, api.CreateJobRequest{Name: "http-reminder", ScheduledFor: &when},
		api.HTTPPayload{URL: *targetURL, Method: "POST", Body: "reminder from the SDK"})
	if err != nil {
		return err
	}
	runs, err := c.ListRuns(ctx, job.ID, 1, 0)
	if err != nil {
		return err
	}
	if len(runs) != 1 {
		return fmt.Errorf("job %s has no initial run", job.ID)
	}
	run, err := c.Wait(ctx, runs[0].ID)
	if err != nil {
		return err
	}
	fmt.Printf("job=%s run=%s status=%s\n", job.ID, run.ID, run.Status)
	return nil
}
