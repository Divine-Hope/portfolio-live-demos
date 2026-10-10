# 0010. Keep 2 GB, run it as a Spot host in an Auto Scaling Group of one

Date: 2026-10-07
Status: Accepted

## Context

The plan was a measured week on the t4g.small before deciding its size. The data
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

A Spot host can be taken away. Losing the host used to mean a manual rebuild; since the self-restore work
it doesn't. A new host restores itself from the Parquet archive and the stream with no
manual steps, in 7 min 50 s in the drill, with the chart continuous afterwards.

## Decision

- **Size:** stay at 2 GB, Graviton.
- **Shape:** the host is the only instance of an Auto Scaling Group (min 1, max 2,
  desired 1) across three zones. If it's reclaimed or fails its EC2 health check, the group
  launches a replacement, and Capacity Rebalancing starts one as soon as AWS warns a Spot
  host is at risk. A launch hook keeps the replacement out of service until it's live: it
  restores itself, waits until its newest event is under a minute old (20 minutes at most,
  so a Wikimedia outage can't block it), takes the Elastic IP, and only then completes the
  hook. If its stack doesn't start, it abandons the hook and the group tries again. Every
  launch and termination is emailed.
- **Purchase:** on demand (t4g.small, free) until the free trial ends on 31 Dec 2026, then
  Spot (`on_demand = false`, then an instance refresh) with price-capacity-optimized
  allocation over t4g.small, c6g.medium and c7g.medium. All three are 2 GB Graviton; T
  types get standard CPU credits, through a second launch template.
- **Disk:** 16 GB instead of 25 (5.5 GB used).

## Consequences

- About $5 a month until the end of 2026. From 2027 on Spot, $11.12 to $16.23 a month
  (EBS and IPv4 included) depending on which type the allocation picks, at today's
  prices: t4g.small or c6g.medium at the low end, c7g.medium at the top. Against $18.50 on
  demand. Still over $10: the public IPv4 address alone is $3.65.
- Each Spot reclaim costs up to about 8 minutes of "Paused" when the host goes before its
  replacement is live, and none when the rebalance warning comes early enough: the old host
  is only retired once the replacement is in service. Plus a few emails.
- Two hosts overlap during a replacement, each with its own ClickHouse. Only the one
  holding the Elastic IP archives (it checks its public IP each run), and the archive also
  reads a file back before replacing it, so neither can shrink a file. Deploys go to both.
- A replacement has 2 days of raw rows, not 7. The page and "Query it" (24 hours on raw
  rows, 3 and 7 days on the rollup) are unaffected; the 90-day rollup comes back whole.
- The per-instance CloudWatch recover and reboot alarms go: the group's health check
  replaces a broken host instead.
- Deploys go to every host the group has put in service.
- What only time can show (freshness over a week, "Query it" latency on the host, origin
  requests against page views, the real bill) is collected by Grafana and Cost Explorer as
  the system runs, not by holding deploys.
- Revisit if interruptions are frequent enough to show on the freshness SLO, or if the
  Spot price for 2 GB Graviton rises above about $0.015 an hour.

## How the move went

The move kept the old single instance running until the group's first host was live:

1. Apply with a temporary `legacy-host.tf` in place: it adds the group and keeps the old
   host. The group's first host restores, goes live and takes the Elastic IP; the old one
   stops getting traffic.
2. Check: `make -s host-id` is the group's host, `/readyz` through CloudFront, and
   `aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names livedemos-host`
   shows it `InService`.
3. Delete `legacy-host.tf` and the `instance-id` deploy parameter, then `make tf-plan
   tf-apply` by hand: it destroys the old host and its two alarms.

What happened, 2026-10-07 to 08 (UTC): The group's first host launched at 23:26, restored 409,510
raw rows and 13 rollup hours from the archive in 5 seconds, and replayed the stream from
22:30. Replay runs at about 4 times real time, so it hadn't caught up by its 15-minute
cap, and it took the Elastic IP stale at 23:43 while the old host was still live; it was
live itself at 23:47. Since then a new host keeps waiting (up to 70 minutes) while another
host holds the Elastic IP. The chart stayed continuous: 23:00 to 23:25 held 2,888 edits on
the new host against 2,919 ingested by the old one (Grafana), within the edge effects of
event time against ingest time. The old host, which had no public IP of its own, lost
its internet access with the Elastic IP, so it stopped serving and archiving at once.
