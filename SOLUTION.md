# Solution

## What was broken, and why

**Duplicate records, call-counts drifting up.** `Ingest` checked `store.EventExists`,
then called `store.InsertEvent` as a separate statement. `events.event_id` had a plain
index, not a unique one, so nothing enforced that assumption between the two calls — two
copies of the same event arriving close together (normal under "at least once" delivery)
could both pass the check before either committed, and both would insert and increment
`account_stats`. The existing `TestDuplicateDeliveryIsIgnored` sends its redeliveries one
at a time, so it never opens the race window. Reproduced with a test that fires 20 copies
at once instead (`TestConcurrentRedeliveryDoesNotDoubleCount`).

**Recordings never marked processed, nothing logged.** `processRecording` ran in a
goroutine holding the *request's* `ctx`. `net/http` cancels that context as soon as the
handler returns, and `Ingest` returns right after launching the goroutine — so by the
time the simulated download finished, `ctx` was already dead and `MarkRecordingProcessed`
failed on essentially every call. The error was dropped by a bare `// TODO: handle`,
which is why nothing showed up in the logs.

**Work vanishing on deploy.** That same goroutine was never tracked. `srv.Shutdown` only
waits for HTTP handlers, and `Ingest` returns before its background work finishes, so on
`SIGTERM` the process could exit — running the deferred `store.Close()`/`redis.Close()` —
mid-goroutine. Fixed together: the goroutine now runs on its own
`context.WithTimeout(context.Background(), …)` instead of the request's context, a
`sync.WaitGroup` tracks it, and a new `Service.Shutdown` blocks on that WaitGroup; `main`
calls it right after `srv.Shutdown`, each with its own timeout so a slow HTTP drain can't
eat into background work's window and reintroduce the same problem one layer up.

**The rest of these I found by re-reviewing my own fix, not from the incident report.**
`IngestEvent`'s insert, call upsert, and stats increment were still three separate
statements: if the first committed and a later one failed for any transient reason,
`Ingest` returned an error, the provider retried the same `event_id` on the non-2xx, and
that retry would hit the unique constraint and be told "already seen" — with no call row
and no stats behind it, silently gone for good. All three now run inside one transaction,
so a failure partway rolls the event insert back too and a retry is treated as new
(`TestIngestEventRollsBackOnFailurePartway` forces this with a `duration_sec` that
overflows Postgres' `INT` column). Separately, `Cache.Get` takes `c.mu.RLock`, but
`Cache.Record` — the write path, hit on every ingest — never touched the mutex; 200
concurrent `Record` calls for one account landed on `CallCount = 191` locally, nine
updates silently lost. And the cache started empty on every restart with nothing to
repopulate it, so stats visibly dropped to zero after every deploy despite `account_stats`
in Postgres being correct throughout. Fixed the lock; added a startup step that loads
`account_stats` into the cache before serving traffic.

## Why this deduplication strategy

**Postgres's unique constraint on `event_id` is the source of truth** — one round trip,
`INSERT … ON CONFLICT (event_id) DO NOTHING`. Two alternatives considered and rejected:

- *Redis alone (`SETNX` as the only gate)* — faster, but not durable enough to be the
  only guard: an eviction, a restart without persistence, or a crash between `SETNX`
  succeeding and the Postgres write committing could let a duplicate through, or worse,
  permanently mark a genuine event "seen" in Redis when Postgres never got it — silently
  dropping a real call.
- *An advisory lock around the old check-then-insert* — closes the race, but is more
  moving parts than the schema-level fix for no extra correctness, and can be forgotten
  on some future code path the way a unique constraint can't.

Redis still sits in front of Postgres, but only as a fast path, ordered so it can't
compromise correctness: `markSeen` writes the dedupe key *after* Postgres confirms the
insert, never before. A Redis hit is therefore always trustworthy; a miss (cold cache,
expired TTL, Redis down) just falls through to the Postgres check, which is correct on
its own. Deleting the Redis layer would only cost latency on repeat redeliveries, never
correctness — `TestRedisFastPathTrustsEarlierConfirmation` verifies this by deleting the
durable row after the first delivery and confirming Postgres is never re-inserted into.

## At 10,000 webhooks/second

Today's path does up to four synchronous round trips (Redis check, insert event, upsert
call, increment stats) before acking — fine at low volume, not at 10k/sec. In order of
impact, I'd:

1. **Take the DB off the request path** — validate, durably enqueue (Kafka / Redis
   Streams / SQS), ack. Workers handle the writes and recording processing, so ingestion
   and processing scale independently.
2. **Stop doing one `UPDATE` per call on `account_stats`** — a busy account currently
   serializes every one of its own webhooks on that row's lock. Buffer increments per
   account (in memory or Redis `INCR`) and flush deltas on a short interval instead.
3. **Batch inserts** — accumulate for tens of milliseconds, write with one multi-row
   `INSERT`/`COPY` instead of one round trip per event.
4. **Let Redis be the primary dedupe gate**, reconciling against Postgres in the worker
   instead of checking it synchronously on every request — worth the small durability
   gap at this volume, with Redis persistence/replication backing it.
5. **Partition `events` by time** and set a retention policy — ~860M rows/day needs an
   archiving strategy, not just an index.

If I kept going, buffered stats writes and queueing off the DB work are the two I'd build
first — everything past that is tuning on top.
