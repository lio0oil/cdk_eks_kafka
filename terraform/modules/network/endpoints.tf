# 機微データやクレデンシャルを運ぶ API だけを PrivateLink に固定する（NAT は残す）。
# 制御メタデータしか流れない API（elb / ec2 / sns 等）は固定費に見合わないため対象外。
locals {
  interface_endpoint_services = var.enable_interface_endpoints ? toset(var.interface_endpoint_services) : toset([])
}

resource "aws_security_group" "interface_endpoint" {
  count = var.enable_interface_endpoints ? 1 : 0

  name        = "${var.cluster_name}-vpc-endpoints"
  description = "Interface VPC Endpoints (HTTPS from within the VPC)"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "interface_endpoint_https" {
  count = var.enable_interface_endpoints ? 1 : 0

  security_group_id = aws_security_group.interface_endpoint[0].id
  description       = "from ${data.aws_vpc.this.cidr_block}:443"
  cidr_ipv4         = data.aws_vpc.this.cidr_block
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "interface_endpoint_all" {
  count = var.enable_interface_endpoints ? 1 : 0

  security_group_id = aws_security_group.interface_endpoint[0].id
  description       = "Allow all outbound traffic by default"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_vpc_endpoint" "interface" {
  for_each = local.interface_endpoint_services

  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.interface_endpoint[0].id]
}
