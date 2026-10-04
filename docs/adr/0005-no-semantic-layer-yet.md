# 0005. No semantic layer (Cube) in v1

Date: 2026-10-04
Status: Accepted

## Context

Cube is built for embedded analytics: a semantic layer, pre-aggregations, and a signed security context per tenant. That's directly relevant to the kind of work this portfolio is about.

## Decision

Not in v1. The API is a thin FastAPI service with a handful of explicit queries.

## Consequences

- Saves 1 to 2 GB of RAM on a 2 GB host, and keeps the live path to one hop (API to ClickHouse).
- Metric definitions live in `api/queries.py` and the widget's definitions text, not in a shared model.
- The demo shows a language filter, not tenant isolation, and says so on the page.
- Revisit for a "multi-tenant embedding" milestone: Cube with a signed security context and tests that prove one tenant can't read another's data.
