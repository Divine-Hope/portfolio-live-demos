# 0006. The resume bookmark is stored on the rows it describes

Date: 2026-10-04
Status: Accepted
Revisions: 2026-10-05, a failed insert is retried unchanged. Before, ingest dropped the batch and re-read the bookmark, and a commit landing after that re-read was written twice under a different token (`test_an_insert_that_lands_after_its_retry_is_not_written_twice` failed 3 times out of 3 against that version).

## Context

EventStreams sends an `id` with every message: a JSON array with a position per stream partition. Send it back as `Last-Event-ID` and the server replays from there. The usual pattern stores this cursor separately (a file, a table, a key-value store). Then the cursor and the data can disagree after a crash: data written but cursor not, or the reverse.

## Decision

Each row in `wiki_edits` stores the `sse_id` it arrived with, plus an `ingest_seq` that orders rows by ingest. A batch is one INSERT, which ClickHouse applies atomically within one partition, so rows and bookmark land together or not at all. On restart, the bookmark is the `sse_id` of the row with the highest `ingest_seq`.

Supporting rules:

- The in-memory bookmark only moves after an insert commits. On a stream error the unsent batch is dropped and replayed.
- A failed insert is different: it may have committed, or may still be running on the server. That batch is sealed (a frozen record of the rows, one token, one query id; nothing changes the rows after sealing) and retried as it is until ClickHouse confirms it. Same token, so whichever attempt lands second is dropped. While the first attempt still runs, its query id is taken and the retry is refused. Only then does the bookmark move.
- On start, ingest waits for inserts a killed predecessor left running (their query ids are prefixed `ingest-`, visible in `system.processes`), for as long as that takes, and only then reads the bookmark. It never resumes past a running insert; it warns every 60 s (`ingest_inflight_waits_total`). The writer profile's 30 s query limit ends any insert in practice. Checked: a SIGKILLed client's insert keeps running server-side. Not reproduced: a duplicate without this wait. Raw rows commit within milliseconds, before the materialized view finishes, so the proof test passes with the wait removed too; the wait closes the window between the server receiving an insert and committing it, which is too short to hit on purpose.
- `ingest_seq` is seeded from the highest committed value, so a clock set back across a restart can't make an older row look newest.
- A batch never spans two UTC days. An INSERT is atomic only within one partition, and the table is partitioned by day.
- The source resumes by timestamp, inclusively, so a few events at the seam come back. The ids of the last 20,000 events *ingested* are checked and skipped. Ingest order, not event time: checked against the real stream on 2026-10-04, a replay interleaves two topics and delivers late events, so `meta.dt` can sit minutes behind an event's place in the stream. A window by event time would miss duplicates; the proof test now runs with events up to 5 minutes late.
- Each batch carries an insert deduplication token built from its event ids (not SSE ids, which two events in the same millisecond can share), so retrying the same batch is a no-op, including in the per-minute rollup.

## Consequences

- No separate cursor to keep consistent. The proof test (`make proof`) slows every insert to ~1.5 s with a test-only view, SIGKILLs ingest while one is provably running, and checks the table against the fake source's ground truth: nothing missing, nothing doubled, rollup equal to raw per minute and language. The fake source interleaves two topics whose positions advance independently, repeats milliseconds, and delivers late events. Two more tests cover an insert that commits but reports failure, and one that fails fast but commits a second later.
- What the tests show: in `wiki_edits`, no event missing and none twice, for every event the source delivers while its position is inside retention. Across a SIGKILL during an insert with an immediate restart, an insert that commits but reports failure, and one that fails fast but commits later. With per-topic positions, shared milliseconds and late events in the source.
- What that rests on, and what isn't covered:
  - One ingest process. Two at once would break the ordering and the seam. Compose runs one.
  - The deduplication window (1,000 inserts). It covers a retry because ingest writes nothing else until the pending batch is confirmed.
  - A seam of 20,000 events. A replay that resends more than the last 20,000 ingested events (hours of traffic) would double the excess.
  - The rollup is written by a materialized view in the same INSERT but not the same transaction. Checked: raw rows are visible before the view finishes. `make reconcile` compares every minute and language against raw rows and, with `REPAIR=1`, stops ingest and rebuilds those that differ. Repair refuses while an ingest insert is running (raw rows can be visible while its view still writes) or rows are still arriving, and fails if either happens during it (`test_repair_refuses_while_an_insert_is_still_writing_the_rollup`). It isn't scheduled: it runs by hand.
- About 150 bytes per row of extra storage before compression (ZSTD), on a table that only keeps 7 days.
- Startup reads the bookmark with bounded queries, whatever the table's size ([architecture](../architecture.md#ingest-srclivedemosingest), [benchmarks](../benchmarks.md)).
- When raw rows have expired but the 90-day rollup still has data, a start is a gap from the rollup's last minute, not a first boot. A bookmark that ages out while ingest stays up (a long outage) is dropped before reconnecting, and the gap recorded.
- Retention, measured 2026-10-04: `?since=` 32 and 45 days back both started at 2026-09-23T09:44Z, so about 11 days of history were available. A `since` older than that starts silently at the oldest event, with no error. `ingest` assumes 7 days (`INGEST_RETENTION_S`), which is inside what was observed. Raising it to match Wikimedia wouldn't help: the bookmark lives on raw rows, and raw rows are kept 7 days, so after a longer outage there's no bookmark to resume from anyway.
