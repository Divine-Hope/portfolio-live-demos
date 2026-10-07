# 0007. One EC2 host running Docker Compose

Date: 2026-10-04
Status: Accepted. The host is now an Auto Scaling Group of one ([0010](0010-spot-host-in-an-auto-scaling-group.md))

## Context

Options: one EC2 instance with Docker Compose; ECS on Fargate; Kubernetes; serverless (Lambda plus API Gateway). The budget is a few dollars a month and the workload is small and steady.

## Decision

One EC2 t4g.small (2 vCPU, 2 GB, Graviton) running the same Compose stack as local development, with CloudFront in front. Terraform for everything. No SSH; SSM for shell and deploys.

## Consequences

- Local and production run the same containers. The t4g.small free trial covers the instance until 31 Dec 2026.
- A single host is a single point of failure. That's accepted: EC2 auto-recovery, restart policies, CloudFront failover to the last S3 snapshot, and rebuild-from-source keep the blast radius small and visible.
- Fargate for the whole stack was estimated at EUR 25 to 40 a month with IPv4, NAT and a load balancer, and ClickHouse wants persistent local disk. Lambda can't hold a long-lived stream consumer (15-minute limit).
- If memory measurements say 2 GB isn't enough, the move to t4g.medium is one Terraform variable.
