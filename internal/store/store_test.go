package store_test

import (
	"context"
	"math"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestInsertEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	inserted, err := s.InsertEventIfNew(ctx, evt)
	if err != nil {
		t.Fatalf("InsertEventIfNew: %v", err)
	}
	if !inserted {
		t.Fatal("expected the first delivery to insert a new row")
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}

	inserted, err = s.InsertEventIfNew(ctx, evt)
	if err != nil {
		t.Fatalf("InsertEventIfNew on redelivery: %v", err)
	}
	if inserted {
		t.Fatal("expected a redelivery of the same event_id to be rejected by the unique constraint")
	}
}

func TestIngestEventStoresEventCallAndStatsTogether(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 90, Payload: []byte(`{}`),
	}

	inserted, err := s.IngestEvent(ctx, evt)
	if err != nil {
		t.Fatalf("IngestEvent: %v", err)
	}
	if !inserted {
		t.Fatal("expected the first delivery to insert")
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event row to exist")
	}

	var gotAccount string
	row := s.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call row for %s: %v", callID, err)
	}

	stats, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 || stats.TotalDurationSec != 90 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=90", stats)
	}

	inserted, err = s.IngestEvent(ctx, evt)
	if err != nil {
		t.Fatalf("IngestEvent on redelivery: %v", err)
	}
	if inserted {
		t.Fatal("expected the redelivery to be rejected")
	}
	stats, err = s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats after redelivery: %v", err)
	}
	if stats.CallCount != 1 {
		t.Fatalf("call_count = %d after a redelivery, want 1 (unchanged)", stats.CallCount)
	}
}

// TestIngestEventRollsBackOnFailurePartway forces UpsertCall to fail inside
// IngestEvent's transaction by giving it a duration_sec Postgres' INT column
// can't hold, and checks that the events row it already wrote in the same
// transaction did not survive. Without the transaction, that insert would
// have already committed on its own statement, and a well-formed retry of
// the same event_id would come back from insertEventIfNew as "already
// seen" - a duplicate that never actually made it into calls or
// account_stats.
func TestIngestEventRollsBackOnFailurePartway(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	// Computed rather than a literal so the overflow is a runtime concern
	// on this (amd64/arm64) build, not a "constant overflows int" compile
	// error on a 32-bit GOARCH where int itself is only 32 bits wide.
	var outOfRange int64 = math.MaxInt32
	outOfRange++

	bad := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: int(outOfRange), // overflows calls.duration_sec (int4)
		Payload: []byte(`{}`),
	}
	if _, err := s.IngestEvent(ctx, bad); err == nil {
		t.Fatal("expected an error from the out-of-range duration_sec")
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("events row survived a transaction that failed on a later statement - it should have rolled back too")
	}

	good := bad
	good.DurationSec = 90
	inserted, err := s.IngestEvent(ctx, good)
	if err != nil {
		t.Fatalf("IngestEvent retry after rollback: %v", err)
	}
	if !inserted {
		t.Fatal("expected the retry to be treated as new, not as an already-seen duplicate")
	}
}

func TestIncrementAccountStatsAccumulates(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}
	if err := s.IncrementAccountStats(ctx, accountID, 12); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestAllAccountStatsReturnsEveryAccount(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	all, err := s.AllAccountStats(ctx)
	if err != nil {
		t.Fatalf("AllAccountStats: %v", err)
	}
	got, ok := all[accountID]
	if !ok {
		t.Fatalf("expected %s in the result", accountID)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 30 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=30", got)
	}
}

func TestUpsertCallThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}
