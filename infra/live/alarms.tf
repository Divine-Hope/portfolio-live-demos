# AWS-level safety net for the host, independent of the stack running on it. Grafana Cloud
# watches the app; these act when the machine itself is broken. Both are inside
# CloudWatch's free tier (10 alarms, basic 1-minute status checks).

resource "aws_sns_topic" "host_alarms" {
  name = "${var.project}-host-alarms"
}

# Email needs confirming once: AWS sends a link to this address after the first apply.
resource "aws_sns_topic_subscription" "host_alarms_email" {
  topic_arn = aws_sns_topic.host_alarms.arn
  protocol  = "email"
  endpoint  = var.budget_email
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
