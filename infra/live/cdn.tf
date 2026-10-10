# CloudFront does the fan-out: one origin request per second, however many viewers.
#
#   /v1/*/live.json  host first, the S3 snapshot if the host fails (1 s cache)
#   /v1/*/activity   host only (10 s cache, keyed on lang and window)
#   /v1/ops.json     host only (60 s cache)
#   /readyz /healthz host only, never cached
#   anything else    the private snapshot bucket, which answers 403
#
# The last line keeps /metrics and /docs off the internet without an extra function.

locals {
  host_origin     = "host"
  snapshot_origin = "snapshots"
  live_origin     = "live-with-fallback"
}

resource "aws_cloudfront_origin_access_control" "snapshots" {
  name                              = "${var.project}-snapshots"
  description                       = "CloudFront reads the fallback snapshot"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

data "aws_cloudfront_cache_policy" "disabled" {
  name = "Managed-CachingDisabled"
}

# Each follows the api's own Cache-Control, within these bounds.
#   live      max-age=1
#   activity  10 s, keyed on the query
#   ops       rebuilt once a minute by the api, so cached for that minute here
locals {
  cache_policies = {
    live     = { default_ttl = 1, max_ttl = 2, query_strings = [] }
    activity = { default_ttl = 10, max_ttl = 10, query_strings = ["lang", "window"] }
    ops      = { default_ttl = 60, max_ttl = 60, query_strings = [] }
  }
}

resource "aws_cloudfront_cache_policy" "api" {
  for_each    = local.cache_policies
  name        = "${var.project}-${each.key}"
  min_ttl     = 0
  default_ttl = each.value.default_ttl
  max_ttl     = each.value.max_ttl

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true

    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = length(each.value.query_strings) > 0 ? "whitelist" : "none"
      dynamic "query_strings" {
        for_each = length(each.value.query_strings) > 0 ? [each.value.query_strings] : []
        content {
          items = query_strings.value
        }
      }
    }
  }
}

moved {
  from = aws_cloudfront_cache_policy.live
  to   = aws_cloudfront_cache_policy.api["live"]
}

moved {
  from = aws_cloudfront_cache_policy.activity
  to   = aws_cloudfront_cache_policy.api["activity"]
}

moved {
  from = aws_cloudfront_cache_policy.ops
  to   = aws_cloudfront_cache_policy.api["ops"]
}

# The api sends CORS headers itself; the S3 fallback doesn't. This adds them to both,
# so the widget can read Date and Age cross-origin whichever origin answered.
resource "aws_cloudfront_response_headers_policy" "cors" {
  name = "${var.project}-cors"

  cors_config {
    access_control_allow_credentials = false
    access_control_max_age_sec       = 600
    origin_override                  = false

    access_control_allow_headers {
      items = ["*"]
    }
    access_control_allow_methods {
      items = ["GET", "HEAD", "OPTIONS"]
    }
    access_control_allow_origins {
      items = ["*"]
    }
    access_control_expose_headers {
      items = ["Age", "Date"]
    }
  }
}

# A web ACL alone is billed monthly, past the budget. Every route is a
# cached, read-only GET, and the origin only answers requests that carry the origin secret.
# Access logs aren't read by anything; Grafana has the API's metrics.
# trivy:ignore:AVD-AWS-0011
# trivy:ignore:AVD-AWS-0010
resource "aws_cloudfront_distribution" "api" {
  enabled         = true
  comment         = "${var.project} data API"
  is_ipv6_enabled = true
  http_version    = "http2and3"
  price_class     = "PriceClass_100" # North America and Europe edges: the cheapest class

  origin {
    origin_id           = local.host_origin
    domain_name         = aws_eip.host.public_dns
    connection_attempts = 1
    connection_timeout  = 2

    # HTTP on purpose: the host has no certificate, its security group only lets CloudFront
    # in, and the header below proves the request came through this distribution. The
    # provider requires https_port and origin_ssl_protocols; under http-only they're unused.
    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "http-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 5
      origin_keepalive_timeout = 5
    }

    # Proves a request came through this distribution. The api refuses anything without it.
    custom_header {
      name  = "X-Origin-Verify"
      value = random_password.secret["api-origin-secret"].result
    }
  }

  origin {
    origin_id                = local.snapshot_origin
    domain_name              = module.snapshots.regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.snapshots.id
  }

  # Connection failures and these status codes from the host send the request to S3.
  origin_group {
    origin_id = local.live_origin

    failover_criteria {
      status_codes = [500, 502, 503, 504]
    }

    member {
      origin_id = local.host_origin
    }
    member {
      origin_id = local.snapshot_origin
    }
  }

  # In this order: CloudFront matches the first pattern that fits.
  dynamic "ordered_cache_behavior" {
    for_each = [
      { path = "/v1/*/live.json", origin = local.live_origin, policy = "live" },
      { path = "/v1/*/activity", origin = local.host_origin, policy = "activity" },
      { path = "/v1/ops.json", origin = local.host_origin, policy = "ops" },
    ]
    content {
      path_pattern               = ordered_cache_behavior.value.path
      target_origin_id           = ordered_cache_behavior.value.origin
      viewer_protocol_policy     = "redirect-to-https"
      allowed_methods            = ["GET", "HEAD", "OPTIONS"]
      cached_methods             = ["GET", "HEAD"]
      cache_policy_id            = aws_cloudfront_cache_policy.api[ordered_cache_behavior.value.policy].id
      response_headers_policy_id = aws_cloudfront_response_headers_policy.cors.id
      compress                   = true
    }
  }

  dynamic "ordered_cache_behavior" {
    for_each = ["/readyz", "/healthz"]
    content {
      path_pattern           = ordered_cache_behavior.value
      target_origin_id       = local.host_origin
      viewer_protocol_policy = "redirect-to-https"
      allowed_methods        = ["GET", "HEAD"]
      cached_methods         = ["GET", "HEAD"]
      cache_policy_id        = data.aws_cloudfront_cache_policy.disabled.id
    }
  }

  default_cache_behavior {
    target_origin_id       = local.snapshot_origin
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = data.aws_cloudfront_cache_policy.disabled.id
  }

  # If both origins fail, don't keep serving the error for the default 10 s.
  dynamic "custom_error_response" {
    for_each = [500, 502, 503, 504]
    content {
      error_code            = custom_error_response.value
      error_caching_min_ttl = 1
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}
