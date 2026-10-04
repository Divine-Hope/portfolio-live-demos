# Infrastructure

Everything in AWS is Terraform, apart from the one-time account setup below. Two stacks:

| Stack | What it creates | State |
|---|---|---|
| `bootstrap/` | The S3 bucket that holds Terraform state for `live/` | Local file, gitignored |
| `live/` | Everything else: budget, network, host, buckets, CloudFront, deploy role | In the S3 bucket |

Region `eu-west-1`. Every resource is tagged `project=livedemos`.

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
