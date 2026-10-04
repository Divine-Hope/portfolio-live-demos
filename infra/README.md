# Infrastructure

Everything in AWS is Terraform, apart from the one-time account setup below. Two stacks:

| Stack | What it creates | State |
|---|---|---|
| `bootstrap/` | The S3 bucket that holds Terraform state for `live/` | Local file, gitignored |
| `live/` | Everything else (below) | In the S3 bucket |

Region `eu-west-1`. Every resource is tagged `project=livedemos`.

`live/` holds:

| File | What |
|---|---|
| `budget.tf` | Monthly budget, emails at 80% actual and 100% forecast |
| `network.tf` | VPC, one public subnet, no NAT gateway. Port 80 open to CloudFront's address ranges only |
| `host.tf`, `user-data.sh.tftpl` | One t4g.small (arm64, Amazon Linux 2023), IMDSv2 only, encrypted disk, no SSH key, an Elastic IP |
| `secrets.tf` | Generated passwords and settings in SSM Parameter Store |
| `storage.tf` | Private buckets for the fallback snapshot and the archive |
| `cdn.tf` | CloudFront: 1 s cache on `live.json`, S3 failover, a secret header the api checks |
| `deploy.tf` | GitHub OIDC and a deploy role that can only run the deploy script on this one host |

Operating it (shell, deploys, rotating secrets, resizing): [`docs/runbook.md`](../docs/runbook.md).

## What it costs

| Item | Monthly |
|---|---|
| t4g.small | Free trial until 31 Dec 2026, then about $12 (us-east-1 list price; Ireland is slightly higher) |
| 25 GB gp3 disk | About $2 |
| Elastic IP (public IPv4) | About $3.60 |
| CloudFront, SSM parameters, Session Manager | Free tier |
| S3 | Cents |

No NAT gateway, no load balancer, no KMS keys, no DynamoDB: each would cost more than everything above.

## Access: SSO, no access keys

Terraform runs with short-lived credentials from IAM Identity Center (`aws sso login`). No access keys exist anywhere, so there's nothing long-lived to leak.

One-time, in the console, because this is what creates the credentials Terraform needs:

1. AWS Organizations: create an organization. The original account becomes the management account (billing and sign-in only).
2. Add a member account, `livedemos`. All the resources below live there.
3. IAM Identity Center, enabled in `eu-west-1`. One user, a permission set, and an assignment to `livedemos`.
4. On your machine: `aws configure sso`, profile name `livedemos`.

Organizations, Identity Center and member accounts cost nothing.

## Running it

```
aws sso login --sso-session bplabs     # once per session
make tf-bootstrap                      # once: creates the state bucket, writes live/backend.hcl
cp infra/live/terraform.tfvars.example infra/live/terraform.tfvars   # then edit it
make tf-init
make tf-lock                           # once, then commit the .terraform.lock.hcl files
make tf-plan                           # read the plan
make tf-apply                          # applies exactly that plan
```

`make tf-validate` checks formatting and syntax without touching AWS. CI runs the same.

## State and locking

State lives in a versioned, encrypted, private bucket. Locking uses S3's own conditional writes (`use_lockfile = true`, Terraform 1.10+): a second `plan` waits until the first releases the `.tflock` object. No DynamoDB table.

## Tearing it down

```
AWS_PROFILE=livedemos terraform -chdir=infra/live destroy
```

The state bucket has `prevent_destroy` on purpose. To remove it too, delete that line, empty the bucket, then destroy `bootstrap/`.
