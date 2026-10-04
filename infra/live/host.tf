# One small host running the same Docker Compose stack as local. No SSH key exists:
# `aws ssm start-session --target <instance id>` gets a shell.

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
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "host" {
  statement {
    sid     = "ReadOwnSettings"
    actions = ["ssm:GetParametersByPath", "ssm:GetParameters", "ssm:GetParameter"]
    resources = [
      "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${local.ssm_prefix}",
      "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${local.ssm_prefix}/*",
    ]
  }

  statement {
    sid       = "WriteFallbackSnapshot"
    actions   = ["s3:PutObject"]
    resources = ["${module.snapshots.arn}/v1/*"]
  }
}

resource "aws_iam_role_policy" "host" {
  name   = "own-settings-and-snapshots"
  role   = aws_iam_role.host.id
  policy = data.aws_iam_policy_document.host.json
}

resource "aws_iam_instance_profile" "host" {
  name = "${var.project}-host"
  role = aws_iam_role.host.name
}

resource "aws_instance" "host" {
  ami                    = data.aws_ssm_parameter.al2023_arm64.insecure_value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public.id
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
    volume_size           = var.root_volume_gb
    encrypted             = true
    delete_on_termination = true
  }

  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    region          = var.region
    ssm_prefix      = local.ssm_prefix
    repo_url        = var.repo_url
    compose_version = var.compose_version
    compose_sha256  = var.compose_sha256
  })

  # User data only runs on first boot, and a new AMI shouldn't rebuild a running host.
  # To rebuild on purpose: terraform apply -replace=aws_instance.host
  lifecycle {
    ignore_changes = [ami, user_data]
  }

  tags = { Name = "${var.project}-host" }

  depends_on = [aws_ssm_parameter.settings, aws_ssm_parameter.secrets, aws_ssm_parameter.image_tag]
}

# A fixed address, so CloudFront's origin name survives a stop/start (e.g. resizing).
resource "aws_eip" "host" {
  domain   = "vpc"
  instance = aws_instance.host.id
  tags     = { Name = "${var.project}-host" }

  depends_on = [aws_internet_gateway.main]
}
