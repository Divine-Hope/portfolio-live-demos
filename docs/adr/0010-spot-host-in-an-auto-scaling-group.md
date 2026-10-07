# 0010. Keep 2 GB, run it as a Spot host in an Auto Scaling Group of one

Date: 2026-10-07
Status: Accepted

## Context

The plan (BPL-68) was a measured week on the t4g.small before deciding its size. The data
from the first day already answers the size question, and the bigger question turned out to
be cost.

**Memory, measured in production on 2026-10-07** (Grafana, peaks over four hours that
included the alert fire drill: ingest stopped and replaying, a ClickHouse restart, the API
failing on purpose):

| Container | Peak | Limit |
|---|---|---|
| ClickHouse | 466 MB | 1,200 MB |
| Alloy | 139 MB | 200 MB |
| API | 93 MB | 256 MB |
| Ingest | 83 MB | 192 MB |
| Archive | 35 MB | 128 MB |

About 816 MB in all, on a 1.84 GB host whose available memory never went below 873 MB.
Swap peaked at 143 MB. No OOM kills. 2 GB is enough; 4 GB would be idle. 1 GB isn't:
ClickHouse alone reached 466 MB.

**Cost.** The budget is $10 a month. eu-west-1 prices on 2026-10-07 (AWS Pricing API and
Spot price history), for 730 hours:

| | Per hour | Per month |
|---|---|---|
| t4g.small on demand | $0.0184 | $13.43, free until 31 Dec 2026 (free trial) |
| t4g.small Spot | $0.0083 to $0.0088 | $6.06 to $6.42 |
| c6g.medium Spot (1 vCPU, 2 GB, not burstable) | $0.0098 to $0.0119 | $7.15 to $8.69 |
| c7g.medium Spot | $0.0153 | $11.17 |
| 16 GB gp3, public IPv4 | | $1.41, $3.65 |

On demand, the stack costs about $18.50 a month from 2027: nearly twice the budget. AWS's
Spot Instance Advisor puts eu-west-1 interruptions at 15 to 20% a month for t4g.small and
under 5% for c6g.medium, c7g.medium and m6g.medium.

A Spot host can be taken away. Losing the host used to mean a manual rebuild; since BPL-66
it doesn't. A new host restores itself from the Parquet archive and the stream with no
manual steps, in 7 min 50 s in the drill, with the chart continuous afterwards.

## Decision

- **Size:** stay at 2 GB, Graviton.
- **Shape:** the host is the only instance of an Auto Scaling Group (min 1, max 2,
  desired 1) across three zones. If it's reclaimed or fails its EC2 health check, the group
  launches a replacement, and Capacity Rebalancing starts one as soon as AWS warns a Spot
  host is at risk. A replacement restores itself, then takes the Elastic IP only once its
  newest event is under a minute old, so CloudFront moves to it when it's live. Every
  launch and termination is emailed.
- **Purchase:** on demand (t4g.small, free) until the free trial ends on 31 Dec 2026, then
  Spot (`on_demand = false`) with price-capacity-optimized allocation over t4g.small,
  c6g.medium, c7g.medium, t4g.medium and m6g.medium. It'll mostly land on the cheap,
  rarely interrupted c6g.medium.
- **Disk:** 16 GB instead of 25 (5.5 GB used).

## Consequences

- About $5 a month until the end of 2026, then about $11 to $14, against $18.50 on
  demand. Still a little over $10: the public IPv4 address alone is $3.65.
- Each Spot reclaim costs about 8 minutes of "Paused", or none when the warning comes early
  enough for the replacement to be live first, plus a few emails. At under 5% a month that's
  rare; the outage budget is honest either way, since the page never fakes data.
- A replacement has 2 days of raw rows, not 7. The page and "Query it" (24 hours on raw
  rows, 3 and 7 days on the rollup) are unaffected; the 90-day rollup comes back whole.
- The per-instance CloudWatch recover and reboot alarms go: the group's health check
  replaces a broken host instead.
- Deploys find the host by tag, and deploy to both during a replacement.
- The measured week shrinks to what only time can show: freshness over a week, "Query it"
  latency on the host, origin requests against page views, and the real bill. They're
  collected by Grafana and Cost Explorer as the system runs, not by holding deploys.
- Revisit if interruptions are frequent enough to show on the freshness SLO, or if the
  Spot price for 2 GB Graviton rises above about $0.015 an hour.
