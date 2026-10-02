data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

resource "aws_iam_role" "admin" {
  name        = var.role_name
  description = "EKS cluster-admin role (Kubernetes system:masters)."

  # 複数の運用者・CI/CD ロールに絞る場合は Principal に ARN を列挙する。
  # 各 Principal は `aws eks get-token` でこの Role を assume してから kubectl を使う。
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "sts:AssumeRole"
    }]
  })
}
