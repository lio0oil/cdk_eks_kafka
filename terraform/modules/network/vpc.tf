data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"

  # Local Zone / Wavelength Zone を AZ として拾わないようにする
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, length(var.private_subnet_cidrs))
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.0"

  name = var.cluster_name
  cidr = var.vpc_cidr
  azs  = local.azs

  public_subnets  = var.public_subnet_cidrs
  private_subnets = var.private_subnet_cidrs

  enable_nat_gateway     = true
  single_nat_gateway     = var.nat_gateways == 1
  one_nat_gateway_per_az = var.nat_gateways > 1

  enable_dns_hostnames = true
  enable_dns_support   = true

  # AWS Load Balancer Controller がサブネットを自動検出するためのタグ
  public_subnet_tags  = { "kubernetes.io/role/elb" = "1" }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = "1" }
}
