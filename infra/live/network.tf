# Public subnets only, one per Availability Zone, and no NAT gateway: a NAT gateway alone
# would cost more than the whole stack. The host reaches the internet through its public
# IP. Three zones, so the Auto Scaling Group can find Spot capacity in any of them.

resource "aws_vpc" "main" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.project }
}

# Lock down the VPC's default security group, so nothing can use it by accident.
resource "aws_default_security_group" "default" {
  vpc_id = aws_vpc.main.id
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = var.project }
}

resource "aws_subnet" "public" {
  for_each = { for i, az in var.availability_zones : az => i }

  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.20.${each.value + 1}.0/24"
  availability_zone = each.key

  tags = { Name = "${var.project}-public-${each.key}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project}-public" }
}

resource "aws_route" "internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

# Only CloudFront may connect, and only on port 80. No SSH: shell access is SSM.
data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

resource "aws_security_group" "host" {
  name        = "${var.project}-host"
  description = "Port 80 from CloudFront only. Outbound open."
  vpc_id      = aws_vpc.main.id
}

resource "aws_vpc_security_group_ingress_rule" "from_cloudfront" {
  security_group_id = aws_security_group.host.id
  description       = "HTTP from CloudFront origin-facing servers"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
  prefix_list_id    = data.aws_ec2_managed_prefix_list.cloudfront.id
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.host.id
  description       = "Wikimedia stream, image pulls, SSM, S3"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# The single host's subnet and its route, kept as the first zone's.
moved {
  from = aws_subnet.public
  to   = aws_subnet.public["eu-west-1a"]
}

moved {
  from = aws_route_table_association.public
  to   = aws_route_table_association.public["eu-west-1a"]
}
