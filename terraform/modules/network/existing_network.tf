# VPC と Subnet は Terraform の管理外（VPC の管理側が用意する）。ID を受け取り、CIDR や AZ を読むだけで、
# 作成・変更はしない。NAT Gateway・ルートテーブル・S3 Gateway Endpoint も VPC の管理側が持つ。
#
# EKS とノードが動くための前提は VPC の管理側が満たす必要がある:
# - DNS サポートと DNS ホスト名が有効であること（Interface Endpoint の private DNS にも必要）
# - 2 つ以上の AZ に subnet があること
# - private subnet からインターネット（NAT など）または必要な VPC Endpoint へ到達できること
#   （ノードの起動と ECR からのイメージ取得のため）
# - Pod の IP を割り当てられる十分な空きがあること
data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_vpc" "this" {
  id = var.vpc_id
}

data "aws_subnet" "private" {
  for_each = toset(var.private_subnet_ids)

  id = each.value
}
