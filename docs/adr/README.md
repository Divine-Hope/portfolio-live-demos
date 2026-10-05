# Architecture decision records

Short notes on decisions that shaped this project: the context, what was decided, and what it costs. New ones copy [template.md](template.md).

| # | Decision | Status |
|---|---|---|
| [0001](0001-no-message-broker.md) | No message broker (Kafka, Redpanda) in v1 | Accepted |
| [0002](0002-poll-a-cached-snapshot.md) | The widget polls a CDN-cached snapshot instead of a push stream | Accepted |
| [0003](0003-clickhouse-serving-store.md) | ClickHouse as the serving store | Accepted |
| [0004](0004-parquet-archive-not-iceberg.md) | Plain Parquet on S3 for history, not Iceberg or DuckLake (yet) | Accepted |
| [0005](0005-no-semantic-layer-yet.md) | No semantic layer (Cube) in v1 | Accepted |
| [0006](0006-bookmark-stored-with-rows.md) | The resume bookmark is stored on the rows it describes | Accepted |
| [0007](0007-single-host-docker-compose.md) | One EC2 host running Docker Compose | Accepted |
| [0008](0008-grafana-cloud-observability.md) | Grafana Cloud for observability, CloudWatch only for AWS-level alarms | Accepted |
| [0009](0009-versioned-migrations-separate-user.md) | Versioned migrations, applied by a user only they run as | Accepted |
