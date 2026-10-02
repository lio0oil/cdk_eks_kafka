output "role_arn" {
  value = aws_iam_role.this.arn
}

output "policy_json" {
  description = "付与したインラインポリシー。Statement が空なら null"
  value       = try(aws_iam_role_policy.this[0].policy, null)
}
