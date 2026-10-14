//go:build chaos

package store

import (
	"context"
	"encoding/json"
)

type ChaosCohortCounts struct {
	Jobs       int  `json:"jobs"`
	Names      int  `json:"names"`
	Runs       int  `json:"runs"`
	Succeeded  int  `json:"succeeded"`
	Running    int  `json:"running"`
	OneRunEach bool `json:"one_run_each"`
}

func (s *Store) ChaosCohortCounts(ctx context.Context, names []string) (ChaosCohortCounts, error) {
	// ponytail: scans job names in a <=60k-job acceptance DB; index names for much larger suites.
	var counts ChaosCohortCounts
	err := s.pool.QueryRow(ctx, `WITH cohort AS (
		SELECT j.id, j.name, count(r.id) AS runs,
			count(r.id) FILTER (WHERE r.status='SUCCEEDED') AS succeeded,
			count(r.id) FILTER (WHERE r.status='RUNNING') AS running
		FROM jobs j LEFT JOIN job_runs r ON r.job_id=j.id WHERE j.name=ANY($1::text[])
		GROUP BY j.id,j.name
	) SELECT count(*), count(DISTINCT name), COALESCE(sum(runs),0),
		COALESCE(sum(succeeded),0), COALESCE(sum(running),0), COALESCE(bool_and(runs=1),false)
		FROM cohort`, names).Scan(&counts.Jobs, &counts.Names, &counts.Runs, &counts.Succeeded, &counts.Running, &counts.OneRunEach)
	return counts, err
}

func (s *Store) ChaosCohortEvidence(ctx context.Context, names []string) (json.RawMessage, error) {
	var evidence json.RawMessage
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object(
		'name',j.name,'job_id',j.id,'runs',COALESCE((
			SELECT jsonb_agg(to_jsonb(r) || jsonb_build_object('attempts',COALESCE((
				SELECT jsonb_agg(to_jsonb(a) ORDER BY a.attempt) FROM run_attempts a WHERE a.run_id=r.id
			),'[]'::jsonb)) ORDER BY r.fire_time) FROM job_runs r WHERE r.job_id=j.id
		),'[]'::jsonb)) ORDER BY j.name),'[]'::jsonb) FROM jobs j WHERE j.name=ANY($1::text[])`, names).Scan(&evidence)
	return evidence, err
}
