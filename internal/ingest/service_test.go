package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestDuplicateDeliveryIsIgnored above sends its redeliveries one at a time,
// so each one lands after the previous has already committed — the provider
// promises redelivery, not concurrent redelivery, but in practice a retry
// storm can easily land two copies of the same event back to back before
// either has finished being processed. This test fires a batch of
// deliveries at once to reproduce that: Ingest used to check "does this
// event_id exist" and only then insert it, as two separate round trips, so
// concurrent deliveries could both pass the check before either had
// committed its insert.
func TestConcurrentRedeliveryDoesNotDoubleCount(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const deliveries = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	statusCodes := make([]int, deliveries)
	postErrs := make([]error, deliveries)

	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // line every goroutine up before releasing them together
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				postErrs[i] = err
				return
			}
			statusCodes[i] = resp.StatusCode
			_ = resp.Body.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range statusCodes {
		if postErrs[i] != nil {
			t.Fatalf("delivery %d: post: %v", i, postErrs[i])
		}
		if statusCodes[i] != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, statusCodes[i])
		}
	}

	var eventRows int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventRows); err != nil {
		t.Fatalf("scan events: %v", err)
	}
	if eventRows != 1 {
		t.Fatalf("stored %d copies of %s after %d concurrent deliveries, want 1", eventRows, eventID, deliveries)
	}

	var callCount, totalDuration int64
	row = st.Pool().QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&callCount, &totalDuration); err != nil {
		t.Fatalf("scan account_stats: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("account_stats.call_count = %d after %d redeliveries of one event, want 1", callCount, deliveries)
	}
	if totalDuration != 143 {
		t.Fatalf("account_stats.total_duration_sec = %d, want 143 (one call's worth, not %d)", totalDuration, deliveries)
	}
}

// TestRecordingIsMarkedProcessedAfterRequestReturns posts a webhook with a
// recording_url and then polls the database, the same way an operator
// watching the dashboard would. Recording processing is deliberately
// asynchronous - the handler returns before it's done - so the assertion
// has to wait for it rather than checking immediately after post() returns.
func TestRecordingIsMarkedProcessedAfterRequestReturns(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	var processed bool
	for time.Now().Before(deadline) {
		row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !processed {
		t.Fatal("recording_processed never became true after the handler returned")
	}
}

// TestShutdownWaitsForInFlightRecordingProcessing goes at the service
// directly rather than through an HTTP request, so ctx here is
// context.Background() for the whole test - it never gets cancelled the way
// a request's context does. That isolates what this test is actually
// checking: that Shutdown blocks on the background goroutine's WaitGroup
// instead of returning immediately. Shutdown returning is the only signal
// the caller (main, on SIGTERM) gets that it's now safe to close the store
// and Redis connections that goroutine depends on.
func TestShutdownWaitsForInFlightRecordingProcessing(t *testing.T) {
	svc, st := testutil.NewService(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  5,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now(),
	}
	if err := svc.Ingest(context.Background(), evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(context.Background(),
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to already be true once Shutdown returns")
	}
}

// TestRedisFastPathTrustsEarlierConfirmation checks that the Redis dedupe
// key is actually consulted, not just written. It deletes the durable
// events row behind the service's back after the first delivery - if
// Postgres were being asked again on the redelivery below, it would see no
// matching event_id and happily insert a second one. It stays at zero rows
// instead, which is only possible if Redis's "already seen" answer short-
// circuited the request before Postgres was ever asked.
func TestRedisFastPathTrustsEarlierConfirmation(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("first delivery: got %d, want 200", resp.StatusCode)
	}

	if _, err := st.Pool().Exec(ctx, `DELETE FROM events WHERE event_id = $1`, eventID); err != nil {
		t.Fatalf("delete event: %v", err)
	}

	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("redelivery: got %d, want 200", resp.StatusCode)
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("events has %d rows for %s after the redelivery, want 0 - "+
			"postgres was re-inserted into, so the redis dedupe key was not actually checked", n, eventID)
	}
}
