output "table_bucket_arn" {
  value = module.s3tables.table_bucket_arn
}

output "namespace_name" {
  value = module.s3tables.namespace_name
}

output "event_table_name" {
  value = module.s3tables.event_table_name
}

output "dlq_table_name" {
  value = module.s3tables.dlq_table_name
}

output "checkpoint_bucket_name" {
  value = module.s3tables.checkpoint_bucket_name
}
