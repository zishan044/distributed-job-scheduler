package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCronValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutate   func(*Job)
		firstRun bool
	}{
		{"malformed", func(j *Job) { j.CronExpr = "not a cron expression here" }, false},
		{"seconds", func(j *Job) { j.CronExpr = "0 * * * * *" }, false},
		{"descriptor", func(j *Job) { j.CronExpr = "@daily" }, false},
		{"interval", func(j *Job) { j.CronExpr = "@every 1m" }, false},
		{"inline timezone", func(j *Job) { j.CronExpr = "CRON_TZ=UTC * * * * *" }, false},
		{"incomplete timezone prefix", func(j *Job) { j.CronExpr = "TZ=UTC" }, false},
		{"range", func(j *Job) { j.CronExpr = "60 * * * *" }, false},
		{"zero step", func(j *Job) { j.CronExpr = "*/0 * * * *" }, false},
		{"timezone", func(j *Job) { j.Timezone = "Invalid/Zone" }, false},
		{"local timezone", func(j *Job) { j.Timezone = "Local" }, false},
		{"policy", func(j *Job) { j.MisfirePolicy = "unknown" }, false},
		{"zero override", func(j *Job) { j.NextFireAt = new(time.Time) }, false},
		{"cron with one-shot time", func(j *Job) {}, true},
		{"missing one-shot time", func(j *Job) { j.CronExpr = "" }, false},
		{"one-shot timezone", func(j *Job) { j.CronExpr, j.Timezone = "", "Invalid/Zone" }, true},
		{"one-shot policy", func(j *Job) { j.CronExpr, j.MisfirePolicy = "", "unknown" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := Job{Name: "cron", Type: "handler", Payload: json.RawMessage(`{"handler":"noop"}`), CronExpr: "* * * * *"}
			tc.mutate(&job)
			var first *time.Time
			if tc.firstRun {
				at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				first = &at
			}
			if _, err := prepareJob(job, first); !errors.Is(err, ErrInvalidJob) {
				t.Fatalf("prepareJob() error = %v; want ErrInvalidJob", err)
			}
		})
	}
}

func TestCronNextFire(t *testing.T) {
	for _, tc := range []struct {
		name, expression, timezone, now, want string
	}{
		{"default UTC", "* * * * *", "", "2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"},
		{"fractional second", "* * * * *", "UTC", "2026-01-01T00:00:59.999999Z", "2026-01-01T00:01:00Z"},
		{"Dhaka", "0 9 * * *", "Asia/Dhaka", "2026-01-01T00:00:00Z", "2026-01-01T03:00:00Z"},
		{"names and steps", "*/15 9-17 * * MON-FRI", "UTC", "2026-01-02T09:01:00Z", "2026-01-02T09:15:00Z"},
		{"leap day", "0 0 29 2 *", "UTC", "2026-03-01T00:00:00Z", "2028-02-29T00:00:00Z"},
		{"spring DST gap", "30 2 * * *", "America/New_York", "2026-03-08T06:59:00Z", "2026-03-09T06:30:00Z"},
		{"fall DST first tick", "30 1 * * *", "America/New_York", "2026-11-01T05:00:00Z", "2026-11-01T05:30:00Z"},
		{"fall DST repeated tick", "30 1 * * *", "America/New_York", "2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job, err := prepareJob(Job{Name: "cron", Type: "handler", Payload: json.RawMessage(`{}`),
				CronExpr: tc.expression, Timezone: tc.timezone, Paused: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			now, err := time.Parse(time.RFC3339Nano, tc.now)
			if err != nil {
				t.Fatal(err)
			}
			if err := initializeCron(&job, now); err != nil {
				t.Fatal(err)
			}
			if got := job.NextFireAt.Format(time.RFC3339); got != tc.want || job.MisfirePolicy != "fire_once" {
				t.Fatalf("next fire = %s, policy = %s; want %s, fire_once", got, job.MisfirePolicy, tc.want)
			}
		})
	}
}

func TestCronOverrideAndImpossibleSchedule(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	override := now.Add(-time.Hour).Add(17 * time.Second)
	for _, policy := range []string{"fire_once", "skip", "fire_all"} {
		job, err := prepareJob(Job{Name: "override", Type: "handler", Payload: json.RawMessage(`{}`),
			CronExpr: "* * * * *", NextFireAt: &override, MisfirePolicy: policy}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := initializeCron(&job, now); err != nil || !job.NextFireAt.Equal(override) {
			t.Fatalf("override = %v, error = %v; want %s", job.NextFireAt, err, override)
		}
	}
	for _, at := range []*time.Time{nil, &override} {
		job := Job{CronExpr: "0 0 31 2 *", Timezone: "UTC", NextFireAt: at}
		if err := initializeCron(&job, now); !errors.Is(err, ErrInvalidJob) {
			t.Fatalf("impossible schedule error = %v; want ErrInvalidJob", err)
		}
	}
}
