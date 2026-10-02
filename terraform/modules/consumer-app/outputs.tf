output "function_name" {
  value = aws_lambda_function.consumer.function_name
}

output "dlq_bucket_name" {
  value = module.dlq_bucket.bucket
}
