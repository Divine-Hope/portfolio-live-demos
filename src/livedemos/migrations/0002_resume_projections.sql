-- Find the resume bookmark without scanning by event time (ADR 0006).
--
-- `seq_max` answers max(ingest_seq) from one row per part. `by_ingest_seq` keeps the
-- columns resume needs sorted by ingest order, so the tail of what was ingested last is a
-- primary-key range read, however late or out of order those events were.

ALTER TABLE {database}.wiki_edits
    ADD PROJECTION IF NOT EXISTS seq_max (SELECT max(ingest_seq));

ALTER TABLE {database}.wiki_edits
    ADD PROJECTION IF NOT EXISTS by_ingest_seq (SELECT event_id, sse_id, ingest_seq ORDER BY ingest_seq);

-- Build both for parts written before this migration. Idempotent.
ALTER TABLE {database}.wiki_edits MATERIALIZE PROJECTION seq_max SETTINGS mutations_sync = 1;

ALTER TABLE {database}.wiki_edits MATERIALIZE PROJECTION by_ingest_seq SETTINGS mutations_sync = 1;
