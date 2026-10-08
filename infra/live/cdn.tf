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

# Follows the api's own Cache-Control (max-age=1), within these bounds.
resource "aws_cloudfront_cache_policy" "live" {
  name        = "${var.project}-live"
  min_ttl     = 0
  default_ttl = 1
  max_ttl     = 2

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
      query_string_behavior = "none"
    }
  }
}

resource "aws_cloudfront_cache_policy" "activity" {
  name        = "${var.project}-activity"
  min_ttl     = 0
  default_ttl = 10
  max_ttl     = 10

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
      query_string_behavior = "whitelist"
      query_strings {
        items = ["lang", "window"]
      }
    }
  }
}

# The Ops tab: rebuilt once a minute by the api, so cached for that minute here.
resource "aws_cloudfront_cache_policy" "ops" {
  name        = "${var.project}-ops"
  min_ttl     = 0
  default_ttl = 60
  max_ttl     = 60

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
      query_string_behavior = "none"
    }
  }
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

  ordered_cache_behavior {
    path_pattern               = "/v1/*/live.json"
    target_origin_id           = local.live_origin
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD", "OPTIONS"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = aws_cloudfront_cache_policy.live.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.cors.id
    compress                   = true
  }

  ordered_cache_behavior {
    path_pattern               = "/v1/*/activity"
    target_origin_id           = local.host_origin
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD", "OPTIONS"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = aws_cloudfront_cache_policy.activity.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.cors.id
    compress                   = true
  }

  ordered_cache_behavior {
    path_pattern               = "/v1/ops.json"
    target_origin_id           = local.host_origin
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD", "OPTIONS"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = aws_cloudfront_cache_policy.ops.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.cors.id
    compress                   = true
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
