//go:build chaos

package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type ChaosDispatchBarrier struct {
	PID int
	tx  pgx.Tx
	s   *Store
}

type ChaosBlockedDispatch struct {
	PID     int    `json:"pid"`
	Address string `json:"address"`
}

func (s *Store) ChaosHoldDispatch(ctx context.Context) (*ChaosDispatchBarrier, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	b := &ChaosDispatchBarrier{tx: tx, s: s}
	if err = tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&b.PID); err == nil {
		_, err = tx.Exec(ctx, `LOCK TABLE job_runs IN SHARE MODE`)
	}
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Release(cleanupCtx)
		return nil, err
	}
	return b, nil
}

func (b *ChaosDispatchBarrier) Release(ctx context.Context) error {
	err := b.tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}

func (b *ChaosDispatchBarrier) Blocked(ctx context.Context) (ChaosBlockedDispatch, error) {
	var blocked ChaosBlockedDispatch
	err := b.s.pool.QueryRow(ctx, `SELECT pid, host(client_addr) FROM pg_stat_activity
		WHERE wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid))
		AND query LIKE '%UPDATE job_runs SET status=''QUEUED''%'
		ORDER BY pid LIMIT 1`, b.PID).Scan(&blocked.PID, &blocked.Address)
	return blocked, err
}

func (s *Store) ChaosBackendAlive(ctx context.Context, pid int) (bool, error) {
	var alive bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)`, pid).Scan(&alive)
	return alive, err
}
