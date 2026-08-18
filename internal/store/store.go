// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so the statement
// helpers below can run standalone or as part of a transaction without
// being written twice.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// insertEventIfNew stores the raw delivery and reports whether this call is
// the one that actually inserted it. events.event_id is unique, so a
// redelivered event hits ON CONFLICT DO NOTHING and comes back as
// inserted=false instead of a duplicate row - the check and the insert are
// the same round trip, so there's no window for two concurrent deliveries
// to both think they're first.
func insertEventIfNew(ctx context.Context, q querier, e Event) (inserted bool, err error) {
	tag, err := q.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// upsertCall creates or refreshes the call record for this event.
func upsertCall(ctx context.Context, q querier, e Event) error {
	_, err := q.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// incrementAccountStats folds one completed call into the durable aggregate.
func incrementAccountStats(ctx context.Context, q querier, accountID string, durationSec int) error {
	_, err := q.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// InsertEventIfNew is insertEventIfNew run standalone against the pool. Kept
// around for the tests that exercise it directly; Ingest goes through the
// transactional IngestEvent below instead.
func (s *Store) InsertEventIfNew(ctx context.Context, e Event) (inserted bool, err error) {
	return insertEventIfNew(ctx, s.pool, e)
}

// UpsertCall is upsertCall run standalone against the pool.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	return upsertCall(ctx, s.pool, e)
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

// IncrementAccountStats is incrementAccountStats run standalone against the
// pool.
func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	return incrementAccountStats(ctx, s.pool, accountID, durationSec)
}

// IngestEvent stores a new event, upserts its call, and folds it into
// account_stats as a single transaction. If event_id is a redelivery,
// nothing past the dedupe check runs and inserted comes back false.
//
// The transaction matters for more than the redelivery case: without it, a
// successful events insert followed by a failing UpsertCall or
// IncrementAccountStats (a dropped connection, a full pool, anything
// transient) would leave the event permanently marked as seen with no call
// row and no stats behind it - the provider's retry would hit
// insertEventIfNew's ON CONFLICT, be told it's a duplicate, and the call
// would be silently dropped for good. Wrapping all three writes together
// means a failure partway rolls back the events insert too, so a retry
// after a partial failure is treated as new and the whole sequence runs
// again instead of being waved through as "already handled".
func (s *Store) IngestEvent(ctx context.Context, e Event) (inserted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once Commit has succeeded

	inserted, err = insertEventIfNew(ctx, tx, e)
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, tx.Commit(ctx)
	}

	if err := upsertCall(ctx, tx, e); err != nil {
		return false, err
	}
	if err := incrementAccountStats(ctx, tx, e.AccountID, e.DurationSec); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

// AllAccountStats reads every account's durable aggregate, keyed by
// account_id. Used once at startup to rehydrate the in-memory cache.
func (s *Store) AllAccountStats(ctx context.Context) (map[string]Stats, error) {
	rows, err := s.pool.Query(ctx, `SELECT account_id, call_count, total_duration_sec FROM account_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]Stats)
	for rows.Next() {
		var accountID string
		var st Stats
		if err := rows.Scan(&accountID, &st.CallCount, &st.TotalDurationSec); err != nil {
			return nil, err
		}
		out[accountID] = st
	}
	return out, rows.Err()
}
