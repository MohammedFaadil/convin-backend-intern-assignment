-- events.event_id only had a non-unique index, so the application was the
-- only thing standing between a redelivered event and a duplicate row - and
-- it did that with a SELECT to check, followed by a separate INSERT, which
-- two concurrent deliveries of the same event_id can both slip through.
--
-- Enforcing uniqueness here means the insert itself can be the check: a
-- single `INSERT ... ON CONFLICT (event_id) DO NOTHING` either stores the
-- event or reports that it was already there, atomically, with no race.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
