# Solution

## What was broken, and why

**Duplicate records, call-counts drifting up.** I found that `Ingest` checked
`store.EventExists`, then called `store.InsertEvent` as a separate statement.
`events.event_id` only had a plain index, not a unique one, so nothing enforced that
assumption between the two calls — two copies of the same event arriving close together
(normal under "at least once" delivery) could both pass the check before either
committed, and both would insert and increment `account_stats`. I noticed the existing
`TestDuplicateDeliveryIsIgnored` sends its redeliveries one at a time, so it never opens
the race window, so I wrote a test that fires 20 copies at once instead
(`TestConcurrentRedeliveryDoesNotDoubleCount`) to reproduce the bug before touching the fix.

**Recordings never marked processed, nothing logged.** I traced this to `processRecording`
running in a goroutine that held onto the *request's* `ctx`. `net/http` cancels that
context the moment the handler returns, and `Ingest` returns right after launching the
goroutine — so by the time the simulated download finished, `ctx` was already dead and
`MarkRecordingProcessed` failed on essentially every call. I also found the error was
being thrown away by a bare `// TODO: handle`, which is exactly why nothing showed up in
the logs.

**Work vanishing on deploy.** I saw that same goroutine was never tracked anywhere.
`srv.Shutdown` only waits for HTTP handlers, and `Ingest` returns before its background
work finishes, so on `SIGTERM` the process could exit — running the deferred
`store.Close()`/`redis.Close()` — mid-goroutine. I fixed both problems together: I gave
the goroutine its own `context.WithTimeout(context.Background(), …)` instead of the
request's context, added a `sync.WaitGroup` to track it, and wrote a new `Service.Shutdown`
that blocks on that WaitGroup. I call it from `main` right after `srv.Shutdown`, each with
its own timeout so a slow HTTP drain can't eat into background work's window and
reintroduce the same problem one layer up.

**The rest of these I found by re-reviewing my own fix, not from the incident report.** I
noticed `IngestEvent`'s insert, call upsert, and stats increment were still three separate
statements: if the first committed and a later one failed for any transient reason,
`Ingest` would return an error, the provider would retry the same `event_id` on the
non-2xx, and that retry would hit the unique constraint and get told "already seen" — with
no call row and no stats behind it, silently gone for good. I fixed this by running all
three inside one transaction, so a failure partway rolls the event insert back too and a
retry is treated as new instead of an already-handled duplicate. I wrote
`TestIngestEventRollsBackOnFailurePartway` to force that failure with a `duration_sec`
that overflows Postgres' `INT` column, and confirmed the rollback actually happens.
Separately, I noticed `Cache.Get` takes `c.mu.RLock`, but `Cache.Record` — the write path,
hit on every single ingest — never touched the mutex at all. I ran 200 concurrent `Record`
calls for one account locally and got `CallCount = 191`: nine updates silently lost to the
race. I added the missing lock. I also noticed the cache started empty on every restart
with nothing to repopulate it, so stats would visibly drop to zero after every deploy even
though `account_stats` in Postgres was correct the whole time — I added a startup step
that loads `account_stats` into the cache before the service starts serving traffic.

## Why I chose this deduplication strategy

I made Postgres's unique constraint on `event_id` the source of truth — one round trip,
`INSERT … ON CONFLICT (event_id) DO NOTHING`. I considered two alternatives and rejected
both:

- *Redis alone (`SETNX` as the only gate)* — faster, but I didn't trust it to be durable
  enough to be the only guard. An eviction, a restart without persistence, or a crash
  between `SETNX` succeeding and the Postgres write committing could let a duplicate
  through, or worse, permanently mark a genuine event "seen" in Redis when Postgres never
  actually got it — silently dropping a real call.
- *An advisory lock around the old check-then-insert* — this would have closed the race,
  but I judged it more moving parts than the schema-level fix for no extra correctness,
  and it's something that could be forgotten on some future code path in a way a unique
  constraint can't be.

I still used Redis, but only as a fast path in front of Postgres, and I deliberately
ordered it so it can't compromise correctness: `markSeen` writes the dedupe key *after*
Postgres confirms the insert, never before. That means a Redis hit is always trustworthy;
a miss (cold cache, expired TTL, Redis down) just falls through to the Postgres check,
which is correct on its own. I could delete the Redis layer entirely and only lose latency
on repeat redeliveries, never correctness — I wrote `TestRedisFastPathTrustsEarlierConfirmation`
to verify this myself, by deleting the durable row after the first delivery and confirming
Postgres is never re-inserted into on the redelivery that follows.

## What I'd change at 10,000 webhooks/second

Right now I do a Redis check, then a Postgres transaction (insert, upsert, increment,
commit), then a Redis write — several synchronous round trips before I ack. That's fine at
low volume, but not at 10k/sec. In order of impact, here's what I'd change:

1. **Take the database off the request path.** I'd have the handler validate and durably
   enqueue the raw payload (Kafka / Redis Streams / SQS), then ack immediately. A pool of
   workers would handle the writes and recording processing independently, so ingestion
   and processing scale separately.
2. **Stop doing one `UPDATE` per call on `account_stats`.** Right now a busy account
   serializes every one of its own webhooks on that row's lock. I'd buffer increments per
   account (in memory or Redis `INCR`) and flush aggregated deltas on a short interval
   instead.
3. **Batch the inserts.** I'd accumulate events for tens of milliseconds and write them
   with one multi-row `INSERT`/`COPY` instead of paying for a round trip per event.
4. **Let Redis be the primary dedupe gate**, reconciling against Postgres in the worker
   instead of checking it synchronously on every request. I'd judge the small durability
   gap worth it at this volume, backed by Redis persistence and replication.
5. **Partition `events` by time and set a retention policy.** At 10k/sec that's roughly
   860M rows a day, which I'd need an archiving strategy for, not just an index.

If I kept going past the time I spent here, buffered stats writes and moving the database
work behind a queue are the two I'd actually build first — everything past that is tuning
on top of those.
