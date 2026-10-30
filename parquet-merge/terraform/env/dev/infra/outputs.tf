output "staging_bucket_name" {
  value = module.merge_infra.staging_bucket_name
}

output "staging_bucket_arn" {
  value = module.merge_infra.staging_bucket_arn
}

output "artifact_bucket_name" {
  value = module.merge_infra.artifact_bucket_name
}

output "artifact_bucket_arn" {
  value = module.merge_infra.artifact_bucket_arn
}

output "emr_application_id" {
  value = module.merge_infra.emr_application_id
}

output "emr_application_arn" {
  value = module.merge_infra.emr_application_arn
}
