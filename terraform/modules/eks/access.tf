locals {
  admin_principals = merge(
    { admin = var.admin_role_arn },
    { for i, arn in var.console_role_arns : "console-${i}" => arn },
  )
}

resource "aws_eks_access_entry" "this" {
  for_each = local.admin_principals

  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "cluster_admin" {
  for_each = local.admin_principals

  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  policy_arn    = "arn:${local.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.this]
}
