package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordIsSafeForConcurrentUse fires Record from many goroutines at
// once, the way concurrent webhook deliveries for the same account would.
// Get takes c.mu.RLock, but Record doesn't take the lock at all - so this
// races on the map write and on the struct field increments. Run with
// `go test -race` to get a definitive failure; without -race it's still
// likely to either lose increments (CallCount below want) or crash the test
// binary outright with Go's own "fatal error: concurrent map writes".
func TestCacheRecordIsSafeForConcurrentUse(t *testing.T) {
	c := stats.NewCache()

	const writers = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c.Record("acc_shared", 1)
		}()
	}
	close(start)
	wg.Wait()

	got := c.Get("acc_shared")
	if got.CallCount != writers {
		t.Fatalf("CallCount = %d after %d concurrent Record calls, want %d (lost an update - Record isn't holding the lock)",
			got.CallCount, writers, writers)
	}
	if got.TotalDurationSec != writers {
		t.Fatalf("TotalDurationSec = %d, want %d", got.TotalDurationSec, writers)
	}
}

func TestCacheSeedLoadsDurableTotals(t *testing.T) {
	c := stats.NewCache()
	c.Seed(map[string]stats.AccountStats{
		"acc_1": {CallCount: 7, TotalDurationSec: 210},
	})

	got := c.Get("acc_1")
	if got.CallCount != 7 || got.TotalDurationSec != 210 {
		t.Fatalf("acc_1: got %+v, want CallCount=7 TotalDurationSec=210", got)
	}

	// Seeding one account should not disturb totals recorded for another,
	// whether they came before Seed or after it.
	c.Record("acc_2", 3)
	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 3 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=3", other)
	}
}
