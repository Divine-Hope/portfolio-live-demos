# One small host running the same Docker Compose stack as local, kept alive by an Auto
# Scaling Group of exactly one (ADR 0010). If it's reclaimed (Spot) or fails its health
# check, the group launches another, which restores itself from the archive (the same path
# as the rebuild drill) and only then takes the Elastic IP, so CloudFront moves to it when
# it's live. No SSH key exists: `aws ssm start-session --target <instance id>` gets a shell;
# `make host-id` finds the id.

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "host" {
  name               = "${var.project}-host"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

# Session Manager shell and Run Command (used by deploys).
resource "aws_iam_role_policy_attachment" "host_ssm" {
  role       = aws_iam_role.host.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "host" {
  statement {
    sid     = "ReadOwnSettings"
    actions = ["ssm:GetParametersByPath", "ssm:GetParameters", "ssm:GetParameter"]
    resources = [
      "arn:${local.partition}:ssm:${var.region}:${local.account_id}:parameter${local.ssm_prefix}",
      "arn:${local.partition}:ssm:${var.region}:${local.account_id}:parameter${local.ssm_prefix}/*",
    ]
  }

  statement {
    sid       = "WriteFallbackSnapshot"
    actions   = ["s3:PutObject"]
    resources = ["${module.snapshots.arn}/v1/*"]
  }

  # Takes the Elastic IP once its stack is live (user data). Only that address, and only
  # for instances of this group.
  statement {
    sid       = "TakeTheElasticIp"
    actions   = ["ec2:AssociateAddress"]
    resources = [aws_eip.host.arn]
  }

  statement {
    sid       = "TakeTheElasticIpForThisGroup"
    actions   = ["ec2:AssociateAddress"]
    resources = ["arn:${local.partition}:ec2:${var.region}:${local.account_id}:instance/*"]
    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Name"
      values   = [local.host_group]
    }
  }

  statement {
    sid       = "TakeTheElasticIpOnItsInterface"
    actions   = ["ec2:AssociateAddress"]
    resources = ["arn:${local.partition}:ec2:${var.region}:${local.account_id}:network-interface/*"]
    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Name"
      values   = [local.host_group]
    }
  }

  # Is the Elastic IP already on a live host? Then a new one waits for fresh data instead
  # of taking it early (user data). No resource-level permissions for Describe*.
  statement {
    sid       = "SeeWhoHasTheElasticIp"
    actions   = ["ec2:DescribeAddresses"]
    resources = ["*"]
  }

  # Say when a new host is live (or can't be), for the group's launch hook.
  statement {
    sid       = "CompleteItsLaunch"
    actions   = ["autoscaling:CompleteLifecycleAction"]
    resources = [local.host_group_arn]
  }

  # The hourly Parquet archive, written and read back by ClickHouse's s3() with these
  # credentials. No delete: the host can add and rewrite hours, never remove them.
  statement {
    sid       = "ReadWriteArchive"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${module.archive.arn}/wikipedia/*"]
  }

  # Month-to-date cost for the Ops tab, once a day (ops/cost.py). The one Cost Explorer
  # action it needs; Cost Explorer has no resource-level permissions, hence "*".
  statement {
    sid       = "ReadMonthToDateCost"
    actions   = ["ce:GetCostAndUsage"]
    resources = ["*"]
  }

  # Each day's claim on that call, and its answer, so only one host ever asks.
  statement {
    sid       = "ClaimTheDailyCostCall"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${module.archive.arn}/ops/cost/*"]
  }

  statement {
    sid       = "ListArchive"
    actions   = ["s3:ListBucket"]
    resources = [module.archive.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["wikipedia/*"]
    }
  }
}

resource "aws_iam_role_policy" "host" {
  name   = "host-runtime"
  role   = aws_iam_role.host.id
  policy = data.aws_iam_policy_document.host.json

  # A new name replaces the policy. Create the new one first, so the host never runs
  # without its permissions.
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_instance_profile" "host" {
  name = "${var.project}-host"
  role = aws_iam_role.host.name
}

locals {
  host_group = "${var.project}-host"
  host_group_arn = join(":", [
    "arn:${local.partition}:autoscaling:${var.region}:${local.account_id}",
    "autoScalingGroup:*:autoScalingGroupName/${local.host_group}",
  ])
  # CPU credits only exist for T types; any other type launched with the setting fails.
  # So two launch templates, the same but for that.
  launch_templates = { burstable = true, fixed = false }
}

resource "aws_launch_template" "host" {
  for_each = local.launch_templates

  name_prefix            = "${var.project}-host-${each.key}-"
  image_id               = data.aws_ssm_parameter.al2023_arm64.insecure_value
  update_default_version = true

  iam_instance_profile {
    name = aws_iam_instance_profile.host.name
  }

  # A public IP of its own, for the internet until it takes the Elastic IP.
  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [aws_security_group.host.id]
    delete_on_termination       = true
  }

  # Standard credits: a runaway process slows down instead of running up a bill.
  dynamic "credit_specification" {
    for_each = each.value ? [1] : []
    content {
      cpu_credits = "standard"
    }
  }

  # IMDSv2 only. Hop limit 2 so containers (the api writing to S3) can reach it.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_type           = "gp3"
      volume_size           = var.root_volume_gb
      encrypted             = true
      delete_on_termination = true
    }
  }

  monitoring {
    enabled = false # detailed monitoring costs extra; Grafana Cloud covers it
  }

  user_data = base64encode(templatefile("${path.module}/user-data.sh.tftpl", {
    region          = var.region
    ssm_prefix      = local.ssm_prefix
    repo_url        = var.repo_url
    compose_version = var.compose_version
    compose_sha256  = var.compose_sha256
    eip_allocation  = aws_eip.host.allocation_id
    host_group      = local.host_group
  }))

  dynamic "tag_specifications" {
    for_each = ["instance", "volume", "network-interface"]
    content {
      resource_type = tag_specifications.value
      tags          = { Name = local.host_group }
    }
  }

  # A new AMI isn't a reason to change anything: hosts install security updates themselves
  # (deploy/host/harden.sh). Refreshing the image is an instance refresh, on purpose.
  lifecycle {
    ignore_changes = [image_id]
  }

  depends_on = [aws_ssm_parameter.settings, aws_ssm_parameter.secrets, aws_ssm_parameter.image_tag]
}

resource "aws_autoscaling_group" "host" {
  name                = local.host_group
  min_size            = 1
  max_size            = 2 # room for a replacement before the old host goes
  desired_capacity    = 1
  vpc_zone_identifier = [for s in aws_subnet.public : s.id]

  # A new host is in service only once it's live and holds the Elastic IP; until then the
  # old one keeps serving. Two hours covers the longest user data can take (runbook, "How a new host goes live").
  initial_lifecycle_hook {
    name                 = "live"
    lifecycle_transition = "autoscaling:EC2_INSTANCE_LAUNCHING"
    default_result       = "ABANDON"
    heartbeat_timeout    = 7200 # the most a hook allows
  }

  health_check_type         = "EC2"
  health_check_grace_period = 300

  # Don't hold the apply until the host is in service: that's up to an hour or two, and
  # the runbook checks it. Terraform's default would fail the apply after 10 minutes.
  wait_for_capacity_timeout = "0"

  # Start a replacement when AWS warns a Spot host is at risk, not only when it's taken.
  capacity_rebalance = true

  mixed_instances_policy {
    instances_distribution {
      on_demand_allocation_strategy            = "prioritized"
      on_demand_base_capacity                  = var.on_demand ? 1 : 0
      on_demand_percentage_above_base_capacity = 0
      spot_allocation_strategy                 = "price-capacity-optimized"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.host["fixed"].id
        version            = "$Latest"
      }

      dynamic "override" {
        for_each = var.instance_types
        content {
          instance_type = override.value
          launch_template_specification {
            launch_template_id = aws_launch_template.host[startswith(override.value, "t") ? "burstable" : "fixed"].id
            version            = "$Latest"
          }
        }
      }
    }
  }

  tag {
    key                 = "project"
    value               = var.project
    propagate_at_launch = true
  }

  # The host must be allowed to complete its hook and take the Elastic IP before it boots.
  depends_on = [aws_iam_role_policy.host]
}

# Launches, terminations and failed launches, by email.
resource "aws_autoscaling_notification" "host" {
  group_names = [aws_autoscaling_group.host.name]
  notifications = [
    "autoscaling:EC2_INSTANCE_LAUNCH",
    "autoscaling:EC2_INSTANCE_TERMINATE",
    "autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
    "autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
  ]
  topic_arn = aws_sns_topic.host_alarms.arn
}

# A fixed address for CloudFront's origin. Whichever host is live holds it: each new one
# takes it in its user data, once its stack is up.
resource "aws_eip" "host" {
  domain = "vpc"
  tags   = { Name = "${var.project}-host" }

  # The host it was attached to before the group existed; the group's host takes it over.
  lifecycle {
    ignore_changes = [instance, network_interface]
  }

  depends_on = [aws_internet_gateway.main]
}
