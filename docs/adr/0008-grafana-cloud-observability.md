# 0008. Grafana Cloud for observability, CloudWatch only for AWS-level alarms

Date: 2026-10-04
Status: Accepted (implementation in M4)

## Context

The services expose Prometheus metrics and JSON logs. They need dashboards, alerts and an outside-in uptime check. Options: CloudWatch only, Amazon Managed Prometheus and Grafana, or Grafana Cloud's free tier.

## Decision

Grafana Alloy on the host ships metrics and logs to Grafana Cloud (free tier: 10k series, 50 GB logs, 14-day retention, alerting included). A synthetic check hits the public URL every minute. CloudWatch is used only for what AWS sees best: EC2 status-check auto-recovery, and AWS Budgets.

## Consequences

- Standard, portable instrumentation (Prometheus format) and one place to look.
- A public dashboard can link from the page's "How it works" tab.
- One more account to manage, and Alloy needs a memory cap on a 2 GB host.
- Revisit if the free tier limits change, or if everything should stay inside AWS.
