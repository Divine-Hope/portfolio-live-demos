# 0002. The widget polls a CDN-cached snapshot instead of a push stream

Date: 2026-10-04
Status: Accepted

## Context

The page should feel live. The obvious design is to push updates to every browser over Server-Sent Events or WebSockets. Through CloudFront that means long-lived connections, heartbeats under the 30 s origin read timeout, compression caveats, and connection limits on a small host.

## Decision

The API builds one snapshot per second and serves it as `live.json` with `Cache-Control: max-age=1`. The widget polls every 2 s. CloudFront caches and collapses requests, so the origin sees about one request per second per edge location however many people are watching. Locally, nginx with a 1 s cache and `proxy_cache_lock` plays the same role.

## Consequences

- Same fan-out as push, with plain HTTP caching and nothing long-lived to break.
- Freshness is a few seconds (target under 5 s end to end) instead of sub-second. For a widget that counts edits per minute, that's fine, and the page shows the real number.
- Failover gets simple. The API returns 503 when it has no snapshot or its snapshot is more than 10 s old (ClickHouse down), and CloudFront then serves the last snapshot from S3.
- A cached or fallback copy can't pass for live. The widget measures the newest event's age against the server's clock (the `Date` header plus `Age`), using the `as_of` inside the payload, so old bytes show as old.
- Revisit if a dataset needs sub-second updates. Push can be added on top without changing the snapshot.
