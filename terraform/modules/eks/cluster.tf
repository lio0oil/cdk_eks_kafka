data "aws_partition" "current" {}

locals {
  partition = data.aws_partition.current.partition
}

resource "aws_iam_role" "cluster" {
  name = "${var.cluster_name}-cluster"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  # vpc-cni / coredns / kube-proxy を self-managed 版の自動 bootstrap に任せず、Managed Addon として
  # 明示管理する（バージョン更新を Addon 単位で制御するため）。自動 bootstrap と違い作成順序を
  # 自前で担保する必要があるため、Addon は addons.tf に集約している。
  bootstrap_self_managed_addons = false

  deletion_protection = var.deletion_protection

  # Control Plane Logs の種別は呼び出し側が決める。controllerManager / scheduler はコスト対監査価値が
  # 低いため、通常は audit / api / authenticator のみにする。
  enabled_cluster_log_types = var.enable_control_plane_logs ? var.control_plane_log_types : []

  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_private_access = var.endpoint_private_access
    endpoint_public_access  = var.endpoint_public_access
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = var.bootstrap_cluster_creator_admin_permissions
  }

  # STANDARD は無償・約 14 か月、EXTENDED は追加課金
  upgrade_policy {
    support_type = var.upgrade_support_type
  }

  depends_on = [aws_iam_role_policy_attachment.cluster]
}
