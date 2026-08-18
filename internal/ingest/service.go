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
// events.event_id is unique, and InsertEventIfNew both checks and inserts in
// one round trip, so there's no gap between "is this new" and "store it"
// for two concurrent deliveries to both slip through.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
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

	inserted, err := s.store.InsertEventIfNew(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	if err := s.store.UpsertCall(ctx, rec); err != nil {
		return err
	}
	if err := s.store.IncrementAccountStats(ctx, rec.AccountID, rec.DurationSec); err != nil {
		return err
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

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
