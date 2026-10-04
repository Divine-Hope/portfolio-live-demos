# 0006. The resume bookmark is stored on the rows it describes

Date: 2026-10-04
Status: Accepted

## Context

EventStreams sends an `id` with every message: a JSON array with a position per stream partition. Send it back as `Last-Event-ID` and the server replays from there. The usual pattern stores this cursor separately (a file, a table, a key-value store). Then the cursor and the data can disagree after a crash: data written but cursor not, or the reverse.

## Decision

Each row in `wiki_edits` stores the `sse_id` it arrived with, plus a monotonic `ingest_seq`. A batch is one INSERT, which ClickHouse applies atomically, so rows and bookmark land together or not at all. On restart, the bookmark is the `sse_id` of the row with the highest `ingest_seq`.

Supporting rules:

- The in-memory bookmark only moves after an insert commits. On any stream error the unflushed batch is dropped and replayed.
- After a database error, ingest re-reads the bookmark from the table before reconnecting. An insert that "failed" (a client timeout, say) may have committed anyway, and only the table knows.
- A batch never spans two UTC days. An INSERT is atomic only within one partition, and the table is partitioned by day.
- The source resumes by timestamp, inclusively, so a few events at the seam come back. The ids of the last 20,000 events *ingested* are checked and skipped. Ingest order, not event time: checked against the real stream on 2026-10-04, a replay interleaves two topics and delivers late events, so `meta.dt` can sit minutes behind an event's place in the stream. A window by event time would miss duplicates; the proof test now runs with events up to 5 minutes late.
- Each batch carries an insert deduplication token built from its event ids (not SSE ids, which two events in the same millisecond can share), so retrying the same batch is a no-op, including in the per-minute rollup.

## Consequences

- No separate cursor to keep consistent. The proof test (`make proof`) SIGKILLs ingest mid-stream and checks the table against the fake source's ground truth: nothing missing, nothing doubled, rollup equal to raw. A second test makes an insert commit and then report failure, and checks nothing is written twice.
- Residual risk: an insert that's still running on the server when ingest has already reloaded its state could land after the reload. Inserts are one-second batches, so the window is small; the seam check catches most of it, and the integrity query in the proof test would show it.
- About 150 bytes per row of extra storage before compression (ZSTD), on a table that only keeps 7 days.
- Startup reads the bookmark with bounded queries: the newest event time comes from part metadata, then the last 20,000 rows by `ingest_seq` within the last day of partitions.
- Retention, measured 2026-10-04: `?since=` 32 and 45 days back both started at 2026-09-23T09:44Z, so about 11 days of history were available. A `since` older than that starts silently at the oldest event, with no error. `ingest` assumes 7 days (`INGEST_RETENTION_S`), which is inside what was observed. Raising it to match Wikimedia wouldn't help: the bookmark lives on raw rows, and raw rows are kept 7 days, so after a longer outage there's no bookmark to resume from anyway.
