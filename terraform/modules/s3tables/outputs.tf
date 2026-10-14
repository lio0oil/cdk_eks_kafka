output "table_bucket_arn" {
  value = aws_s3tables_table_bucket.this.arn
}

output "namespace_name" {
  value = aws_s3tables_namespace.events.namespace
}

output "kms_key_arn" {
  description = "checkpoint バケットなど、同じ信頼境界のバケットが共有する"
  value       = aws_kms_key.s3tables.arn
}
