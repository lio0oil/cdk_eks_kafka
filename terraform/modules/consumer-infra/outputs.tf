output "esm_security_group_id" {
  value = aws_security_group.esm.id
}

output "data_bucket_name" {
  value = module.data_bucket.bucket
}

output "data_bucket_arn" {
  value = module.data_bucket.arn
}

output "artifact_bucket_name" {
  value = module.artifact_bucket.bucket
}

output "artifact_bucket_arn" {
  value = module.artifact_bucket.arn
}
