locals {
  bucket_suffix = "${data.aws_caller_identity.current.account_id}-${var.region}"
}

# The last good live.json, written by the api every 60 s. CloudFront serves it when
# the host is down, so the widget shows "Paused" instead of an error.
module "snapshots" {
  source           = "../modules/private-bucket"
  name             = "${var.project}-snapshots-${local.bucket_suffix}"
  policy_documents = [data.aws_iam_policy_document.snapshots_cloudfront.json]
}

data "aws_iam_policy_document" "snapshots_cloudfront" {
  statement {
    sid       = "CloudFrontReadsSnapshots"
    actions   = ["s3:GetObject"]
    resources = ["${module.snapshots.arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.api.arn]
    }
  }
}

# Hourly Parquet files (M4). Older files move to cheaper storage classes on their own.
module "archive" {
  source = "../modules/private-bucket"
  name   = "${var.project}-archive-${local.bucket_suffix}"
}

# A rewrite replaces an hour's object, and S3 keeps the last write. Versioning keeps the
# one it replaced, for 30 days, so a bad rewrite can be undone.
resource "aws_s3_bucket_versioning" "archive" {
  bucket = module.archive.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket     = module.archive.id
  depends_on = [aws_s3_bucket_versioning.archive]

  rule {
    id     = "cheaper-with-age"
    status = "Enabled"
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
