package store

import (
	"fmt"
	"strings"
	"time"
	_ "time/tzdata" // Keep IANA timezone data available in minimal service images.

	"github.com/robfig/cron/v3"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func parseCron(expression, timezone string) (cron.Schedule, error) {
	if len(strings.Fields(expression)) != 5 {
		return nil, fmt.Errorf("%w: cron_expr must have exactly five fields", ErrInvalidJob)
	}
	// Only the timezone field can set the schedule's location.
	schedule, err := cronParser.Parse("CRON_TZ=" + timezone + " " + expression)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid cron_expr or timezone: %v", ErrInvalidJob, err)
	}
	return schedule, nil
}

// initializeCron uses the transaction's database time, even for paused jobs.
// Explicit next_fire_at overrides remain the first fire's identity.
func initializeCron(job *Job, now time.Time) error {
	schedule, err := parseCron(job.CronExpr, job.Timezone)
	if err != nil {
		return err
	}
	next := schedule.Next(now).UTC()
	if next.IsZero() {
		return fmt.Errorf("%w: cron_expr has no next fire within five years", ErrInvalidJob)
	}
	if job.NextFireAt == nil {
		job.NextFireAt = &next
	}
	return nil
}
