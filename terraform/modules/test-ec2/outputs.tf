output "instance_id" {
  value = aws_instance.this.id
}

output "start_session_command" {
  value = "aws ssm start-session --target ${aws_instance.this.id}"
}

output "transfer_bucket_name" {
  value = module.transfer_bucket.bucket
}
