# 0001. No message broker in v1

Date: 2026-10-04
Status: Accepted

## Context

Real-time pipelines often put Kafka or Redpanda between the source and the database. It buys replay, decoupling and fan-out to several consumers.

Here the source already gives us those properties. Wikimedia EventStreams is backed by Kafka on Wikimedia's side and supports resuming from a bookmark (`Last-Event-ID`) or a timestamp (`since`). There's one consumer. The host has 2 GB of RAM, shared with ClickHouse.

## Decision

No broker. ingest reads the stream and writes straight to ClickHouse. The resume bookmark is committed with the rows ([0006](0006-bookmark-stored-with-rows.md)), so a crash replays from the source.

## Consequences

- One fewer service to run, monitor and size. Redpanda alone would want a large share of the box's memory.
- Replay depends on the source's retention. A gap longer than that is recorded, not hidden.
- Revisit when there's a second independent consumer of the same events, or a source that can't replay.
