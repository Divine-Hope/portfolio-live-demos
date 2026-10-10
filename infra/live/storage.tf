locals {
  bucket_suffix = "${local.account_id}-${var.region}"
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

# Hourly Parquet files. Older files move to cheaper storage classes on their own. A
# rewrite replaces an hour's object, and S3 keeps the last write. Versioning keeps the one
# it replaced, for 30 days, so a bad rewrite can be undone.
module "archive" {
  source     = "../modules/private-bucket"
  name       = "${var.project}-archive-${local.bucket_suffix}"
  versioning = true
  lifecycle_rules = [{
    id = "cheaper-with-age"
    transitions = [
      { days = 30, storage_class = "STANDARD_IA" },
      { days = 180, storage_class = "GLACIER_IR" },
    ]
    noncurrent_version_expiration_days     = 30
    abort_incomplete_multipart_upload_days = 7
  }]
}

