# 2026-10-06. ClickHouse refused work at its memory ceiling while using under half of it

Status: Final

## Summary

After the deploy on 5 October, ClickHouse's own count of the memory it was using drifted upwards until it hit the 900 MiB ceiling we set for it. From then on it refused merges and some queries with `MEMORY_LIMIT_EXCEEDED`, although the container was using 415 MiB. Turning on ClickHouse's memory correction fixed it.

## Impact

From ClickHouse's logs, in the day after the deploy:

- 9,353 `MEMORY_LIMIT_EXCEEDED` errors.
- Merges failed (4 in the last hour before the fix). Left long enough, unmerged parts pile up until ClickHouse slows and then refuses inserts.
- Two API queries failed.

[D] Whether any edits were lost or any viewer saw an error beyond those two queries wasn't recorded at the time.

## Timeline (UTC)

| Time | What happened |
|---|---|
| 5 Oct | Deploy. The server's memory count starts to drift. |
| 6 Oct, 11:00 | The latest `MEMORY_LIMIT_EXCEEDED` before the fix. |
| 6 Oct, 12:39 | Fix merged ([PR #8](https://github.com/Divine-Hope/portfolio-live-demos/pull/8)) and deployed. |

## Cause

ClickHouse tracks its memory use itself and refuses work that would take it over `max_server_memory_usage`. That count drifted to 676 MiB while the container's real (anonymous) memory was 415 MiB, and nothing corrected it: in 25.8, `memory_worker_correct_memory_tracker` is off by default. ClickHouse turns it on by default only from 26.10.

A second problem hid the fix: the config reaches ClickHouse through a bind mount, so a deploy that changed only `clickhouse/` didn't restart it, and the new setting wouldn't have loaded.

## Fix

- `clickhouse/config.d/low-memory.xml` turns on `memory_worker_correct_memory_tracker`, so the count follows the container's real memory ([e2973c9](https://github.com/Divine-Hope/portfolio-live-demos/commit/e2973c9)).
- `deploy/host/deploy.sh` restarts ClickHouse when anything under `clickhouse/` changes.
- ClickHouse upgraded from 25.8 to 26.8 LTS in the same PR, since 25.8 was at the end of its support.

Checked locally on 26.8: the count followed the container's memory, about 300 MiB under the fake stream, where 25.8 had settled at 600 to 630 MiB.

## What we changed so it can't recur

- The setting stays on in our config whatever the version's default.
- Alloy now ships ClickHouse's own count next to the corrected one (`MemoryTracking`, `MemoryTrackingUncorrected`) and its memory-limit refusals (`QueryMemoryLimitExceeded`) to Grafana ([deploy/alloy/config.alloy](../../deploy/alloy/config.alloy)), next to memory per container.
- Not done yet: no alert fires on `QueryMemoryLimitExceeded` itself. The container memory alert wouldn't have caught this, because the container wasn't near its limit.
