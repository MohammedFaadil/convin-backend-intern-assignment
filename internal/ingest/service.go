// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// recordingTimeout bounds how long background recording processing gets to
// run. It's independent of the HTTP request that triggered it, since that
// request is long gone by the time this matters.
const recordingTimeout = 10 * time.Second

// dedupeTTL is how long a confirmed event_id is kept in the Redis fast
// path. It only needs to outlast the provider's redelivery window - letting
// an entry expire early just costs a Postgres round trip on the next
// redelivery, it doesn't cause a double-count, so this is deliberately
// generous.
const dedupeTTL = 24 * time.Hour

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// wg tracks recording-processing goroutines still running in the
	// background, so Shutdown can wait for them instead of letting the
	// process exit out from under them.
	wg sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// The provider redelivers at least once, so this has to tolerate the same
// event_id arriving twice - including two copies arriving at the same time.
// Postgres is the source of truth for that: events.event_id is unique, and
// IngestEvent both checks and inserts in the same transaction as the call
// upsert and the stats update, so there's no gap for two concurrent
// deliveries to both slip through, and no way for a failure partway to
// leave an event marked seen with nothing to show for it.
//
// Redis sits in front of that check purely as a fast path. A dedupe key is
// only ever written after Postgres has confirmed the insert, so a hit in
// Redis is always trustworthy - it can save a round trip to Postgres, but it
// can never cause a new event to be dropped. A miss (cold cache, eviction,
// expired TTL, Redis being down) just means falling through to the Postgres
// check, which is correct on its own. Deleting the Redis layer entirely
// would only cost latency under heavy redelivery, not correctness.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	dedupeKey := "webhook-ingest:seen-event:" + evt.EventID

	if s.rdb != nil {
		seen, err := s.rdb.Exists(ctx, dedupeKey).Result()
		if err != nil {
			s.log.Warn("redis dedupe check failed, falling back to postgres", "event_id", evt.EventID, "err", err)
		} else if seen > 0 {
			s.log.Info("duplicate delivery ignored", "event_id", evt.EventID, "source", "redis")
			return nil
		}
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// IngestEvent stores the event, upserts the call, and folds the call
	// into account_stats as one transaction, so a failure partway through
	// (a dropped connection between the insert and the stats update, say)
	// rolls back the event insert too. Without that, a redelivery after a
	// partial failure would hit the unique constraint, be told it's a
	// duplicate, and the call would be silently dropped rather than retried.
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID, "source", "postgres")
		s.markSeen(ctx, dedupeKey)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)
	s.markSeen(ctx, dedupeKey)

	// Recordings are slow to fetch, so that part does not block the
	// provider. It gets a context of its own rather than ctx: ctx belongs
	// to the HTTP request, and net/http cancels that the moment this
	// handler returns - which is exactly what was cutting the download off
	// before it could mark the recording processed. wg lets Shutdown wait
	// for whatever's still running instead of letting it get killed
	// mid-write when the process exits.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bg, cancel := context.WithTimeout(context.Background(), recordingTimeout)
			defer cancel()
			if err := s.processRecording(bg, rec); err != nil {
				s.log.Error("process recording failed",
					"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// markSeen records a confirmed-durable event_id in Redis so the next
// redelivery can skip Postgres. Best-effort: on failure the dedupe key
// simply doesn't get cached, and the next redelivery pays for a Postgres
// round trip it would otherwise have avoided. That's the only consequence -
// Postgres remains the authority regardless of whether this succeeds.
func (s *Service) markSeen(ctx context.Context, key string) {
	if s.rdb == nil {
		return
	}
	if err := s.rdb.Set(ctx, key, 1, dedupeTTL).Err(); err != nil {
		s.log.Warn("redis dedupe write failed", "key", key, "err", err)
	}
}

// Shutdown waits for recording-processing goroutines started by Ingest to
// finish, or for ctx to expire, whichever comes first. Call it after the
// HTTP server has stopped accepting new work but before closing the store
// or Redis connections those goroutines still need.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
