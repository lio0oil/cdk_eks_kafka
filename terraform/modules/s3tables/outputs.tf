output "table_bucket_arn" {
  value = aws_s3tables_table_bucket.this.arn
}

output "namespace_name" {
  value = aws_s3tables_namespace.events.namespace
}

output "event_table_name" {
  value = awscc_s3tables_table.event.table_name
}

output "dlq_table_name" {
  value = awscc_s3tables_table.dlq.table_name
}

output "checkpoint_bucket_name" {
  value = module.checkpoint_bucket.bucket
}
