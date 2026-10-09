package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrLeaseLost = errors.New("store: worker lease lost")

// RegisterWorker requires a new ID: expired identities must never be reused.
func (s *Store) RegisterWorker(ctx context.Context, id, hostname string, capacity int, lease time.Duration) error {
	worker, err := parseUUID(id)
	if err != nil {
		return err
	}
	if capacity < 1 || capacity > 1000 || lease <= 0 {
		return fmt.Errorf("invalid worker capacity or lease duration")
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, capacity, started_at, last_heartbeat, lease_expires_at)
		VALUES ($1, $2, $3, now(), now(), now() + $4 * interval '1 second')
	`, worker, hostname, capacity, lease.Seconds())
	return err
}

// HeartbeatWorker cannot revive an expired lease, even if it waits for a lock.
func (s *Store) HeartbeatWorker(ctx context.Context, id string, lease time.Duration) (bool, error) {
	worker, err := parseUUID(id)
	if err != nil {
		return false, err
	}
	if lease <= 0 {
		return false, fmt.Errorf("invalid worker lease duration")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT id FROM workers WHERE id=$1 FOR UPDATE`, worker); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE workers SET last_heartbeat=clock_timestamp(),
			lease_expires_at=clock_timestamp() + $2 * interval '1 second'
		WHERE id=$1 AND lease_expires_at > clock_timestamp()
	`, worker, lease.Seconds())
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func workerAlive(ctx context.Context, tx pgx.Tx, worker pgtype.UUID) (bool, error) {
	var alive bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM workers WHERE id=$1 AND lease_expires_at > clock_timestamp()
	)`, worker).Scan(&alive)
	return alive, err
}
