# Postmortems

What broke in production, what it cost, and what changed so it can't happen the same way again. Blameless: the point is the system, not who pushed what. New ones copy [template.md](template.md) and are named `YYYY-MM-DD-short-name.md`.

| Date | What broke | Impact |
|---|---|---|
| [2026-10-06](2026-10-06-clickhouse-memory-drift.md) | ClickHouse refused work at its memory ceiling while using under half of it | Failed merges and two failed API queries; no data lost |
