# Email for the host's lifecycle: the Auto Scaling Group reports every launch and
# termination here (host.tf), so a Spot reclaim or a health-check replacement is never
# silent. The group's EC2 health check replaces a broken host, which is what the old
# recover and reboot alarms did for a single instance. Grafana Cloud watches the app.

# It carries launch and termination notices only. The AWS-managed SNS
# key can't be used by Auto Scaling, and a customer key costs a dollar a month.
# trivy:ignore:AVD-AWS-0095
resource "aws_sns_topic" "host_alarms" {
  name = "${var.project}-host-alarms"
}

# Email needs confirming once: AWS sends a link to this address after the first apply.
resource "aws_sns_topic_subscription" "host_alarms_email" {
  topic_arn = aws_sns_topic.host_alarms.arn
  protocol  = "email"
  endpoint  = var.budget_email
}
