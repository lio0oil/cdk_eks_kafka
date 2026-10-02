output "bucket" {
  description = "バケット名"
  value       = aws_s3_bucket.this.bucket
}

output "arn" {
  value = aws_s3_bucket.this.arn
}

output "policy_json" {
  description = "バケットポリシー。呼び出し側が追加 Statement を検証できるよう出力する"
  value       = aws_s3_bucket_policy.this.policy
}
