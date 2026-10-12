package store

import (
	"testing"
	"time"
)

func TestCronMisfires(t *testing.T) {
	for _, tc := range []struct {
		name, policy, first, now, wantFirst, wantNext string
		limit, count                                  int
	}{
		{"fire once", "fire_once", "2026-01-02T11:57:00Z", "2026-01-02T12:00:02Z", "2026-01-02T11:57:00Z", "2026-01-02T12:01:00Z", 1000, 1},
		{"skip downtime", "skip", "2026-01-02T11:57:00Z", "2026-01-02T12:00:02Z", "", "2026-01-02T12:01:00Z", 1000, 0},
		{"skip grace boundary", "skip", "2026-01-02T12:00:00Z", "2026-01-02T12:00:02Z", "2026-01-02T12:00:00Z", "2026-01-02T12:01:00Z", 1000, 1},
		{"skip beyond grace", "skip", "2026-01-02T12:00:00Z", "2026-01-02T12:00:02.000001Z", "", "2026-01-02T12:01:00Z", 1000, 0},
		{"all missed fires", "fire_all", "2026-01-02T11:57:00Z", "2026-01-02T12:00:02Z", "2026-01-02T11:57:00Z", "2026-01-02T12:01:00Z", 1000, 4},
		{"override then cron ticks", "fire_all", "2026-01-02T11:57:17Z", "2026-01-02T12:00:02Z", "2026-01-02T11:57:17Z", "2026-01-02T12:01:00Z", 1000, 4},
		{"per-job cap", "fire_all", "2026-01-01T13:00:00Z", "2026-01-02T12:00:02Z", "2026-01-01T13:00:00Z", "2026-01-01T14:40:00Z", 1000, 100},
		{"remaining batch budget", "fire_all", "2026-01-01T13:00:00Z", "2026-01-02T12:00:02Z", "2026-01-01T13:00:00Z", "2026-01-01T13:07:00Z", 7, 7},
		{"window clips older ticks", "fire_all", "2025-12-31T12:00:00Z", "2026-01-02T12:00:02Z", "2026-01-01T12:01:00Z", "2026-01-01T13:41:00Z", 1000, 100},
		{"inclusive window boundary", "fire_all", "2025-12-31T12:00:00Z", "2026-01-02T12:00:00Z", "2026-01-01T12:00:00Z", "2026-01-01T13:40:00Z", 1000, 100},
		{"future fire untouched", "fire_once", "2026-01-02T12:01:00Z", "2026-01-02T12:00:02Z", "", "2026-01-02T12:01:00Z", 1000, 0},
		{"due exactly now", "fire_all", "2026-01-02T12:00:00Z", "2026-01-02T12:00:00Z", "2026-01-02T12:00:00Z", "2026-01-02T12:01:00Z", 1000, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parse := func(value string) time.Time {
				t.Helper()
				at, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					t.Fatal(err)
				}
				return at
			}
			first, now := parse(tc.first), parse(tc.now)
			fires, next, err := cronFires(Job{CronExpr: "* * * * *", Timezone: "UTC", MisfirePolicy: tc.policy, NextFireAt: &first}, now, tc.limit)
			if err != nil || len(fires) != tc.count || !next.Equal(parse(tc.wantNext)) {
				t.Fatalf("fires = %v, next = %s, error = %v; want %d fires, next %s", fires, next, err, tc.count, tc.wantNext)
			}
			if len(fires) > 0 && !fires[0].Equal(parse(tc.wantFirst)) {
				t.Fatalf("first fire = %s; want %s", fires[0], tc.wantFirst)
			}
			for i, fire := range fires {
				if fire.After(now) || (i > 0 && !fire.After(fires[i-1])) {
					t.Fatalf("unordered or future catch-up fires: %v", fires)
				}
			}
		})
	}
}

func TestCronMisfiresAcrossDST(t *testing.T) {
	first := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)
	now := first.Add(61 * time.Minute)
	fires, next, err := cronFires(Job{CronExpr: "30 1 * * *", Timezone: "America/New_York", MisfirePolicy: "fire_all", NextFireAt: &first}, now, 100)
	if err != nil || len(fires) != 2 || !fires[0].Equal(first) || !fires[1].Equal(first.Add(time.Hour)) ||
		!next.Equal(time.Date(2026, 11, 2, 6, 30, 0, 0, time.UTC)) {
		t.Fatalf("DST catch-up = %v, next = %s, error = %v", fires, next, err)
	}
}
