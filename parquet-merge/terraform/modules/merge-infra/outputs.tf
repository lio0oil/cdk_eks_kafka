output "staging_bucket_name" {
  value = aws_s3_bucket.this["staging"].bucket
}

output "staging_bucket_arn" {
  value = aws_s3_bucket.this["staging"].arn
}

output "artifact_bucket_name" {
  value = aws_s3_bucket.this["artifact"].bucket
}

output "artifact_bucket_arn" {
  value = aws_s3_bucket.this["artifact"].arn
}

output "emr_application_id" {
  value = aws_emrserverless_application.this.id
}

output "emr_application_arn" {
  value = aws_emrserverless_application.this.arn
}
