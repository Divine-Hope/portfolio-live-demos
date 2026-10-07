# The single host from before the Auto Scaling Group (ADR 0010). Kept for one apply, so the
# group's first host can come up, go live and take the Elastic IP while this one still
# serves. Then delete this file and apply by hand (it destroys the instance and its alarms;
# the pipeline refuses destroys). Runbook: "Moving to the Auto Scaling Group".

resource "aws_instance" "host" {
  ami                    = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type          = "t4g.small"
  subnet_id              = aws_subnet.public["eu-west-1a"].id
  vpc_security_group_ids = [aws_security_group.host.id]
  iam_instance_profile   = aws_iam_instance_profile.host.name
  monitoring             = false # detailed monitoring costs extra; Grafana Cloud covers it (M4)

  # Standard credits: a runaway process slows down instead of running up a bill.
  credit_specification {
    cpu_credits = "standard"
  }

  # IMDSv2 only. Hop limit 2 so containers (the api writing to S3) can reach it.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 25 # as it was; an EBS volume can't shrink
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    region          = var.region
    ssm_prefix      = local.ssm_prefix
    repo_url        = var.repo_url
    compose_version = var.compose_version
    compose_sha256  = var.compose_sha256
    eip_allocation  = aws_eip.host.allocation_id
    host_group      = local.host_group
  })

  # User data only runs on first boot, and a new AMI shouldn't rebuild a running host.
  # To rebuild on purpose: terraform apply -replace=aws_instance.host
  lifecycle {
    ignore_changes = [ami, user_data]
  }

  tags = { Name = "${var.project}-host" }

  depends_on = [aws_ssm_parameter.settings, aws_ssm_parameter.secrets, aws_ssm_parameter.image_tag]
}

# The hardware or AWS's side of it failed: move the instance to healthy hardware. It
# keeps its ID, private IP, Elastic IP and disk, so nothing else changes.
resource "aws_cloudwatch_metric_alarm" "host_recover" {
  alarm_name          = "${var.project}-host-system-check"
  alarm_description   = "System status check failing for 2 minutes: recovering the instance. See docs/runbook.md."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_System"
  dimensions          = { InstanceId = aws_instance.host.id }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  alarm_actions       = ["arn:aws:automate:${var.region}:ec2:recover", aws_sns_topic.host_alarms.arn]
  ok_actions          = [aws_sns_topic.host_alarms.arn]
}

# The OS stopped answering (kernel panic, memory exhausted, network config broken):
# reboot it. The stack comes back by itself (livedemos.service).
resource "aws_cloudwatch_metric_alarm" "host_reboot" {
  alarm_name          = "${var.project}-host-instance-check"
  alarm_description   = "Instance status check failing for 3 minutes: rebooting. See docs/runbook.md."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed_Instance"
  dimensions          = { InstanceId = aws_instance.host.id }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  alarm_actions       = ["arn:aws:automate:${var.region}:ec2:reboot", aws_sns_topic.host_alarms.arn]
  ok_actions          = [aws_sns_topic.host_alarms.arn]
}
