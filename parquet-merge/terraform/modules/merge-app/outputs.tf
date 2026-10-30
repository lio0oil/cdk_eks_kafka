output "function_name" {
  value = aws_lambda_function.writer.function_name
}

output "schedule_name" {
  value = aws_scheduler_schedule.merge.name
}

output "job_role_arn" {
  value = aws_iam_role.job.arn
}

output "schedule_input" {
  value = aws_scheduler_schedule.merge.target[0].input
}

output "role_policies" {
  value = {
    writer    = aws_iam_role_policy.writer.policy
    job       = aws_iam_role_policy.job.policy
    scheduler = aws_iam_role_policy.scheduler.policy
  }
}

# 設定が実際に Lambda / ESM に反映された値。env の配線テストが module 内のリソースを直接参照できないため公開する。
output "applied_settings" {
  value = {
    lambda_timeout_seconds          = aws_lambda_function.writer.timeout
    lambda_memory_mb                = aws_lambda_function.writer.memory_size
    log_retention_days              = aws_cloudwatch_log_group.writer.retention_in_days
    esm_topics                      = tolist(aws_lambda_event_source_mapping.kafka.topics)
    esm_bootstrap_servers           = one(aws_lambda_event_source_mapping.kafka.self_managed_event_source).endpoints["KAFKA_BOOTSTRAP_SERVERS"]
    esm_starting_position           = aws_lambda_event_source_mapping.kafka.starting_position
    esm_batch_size                  = aws_lambda_event_source_mapping.kafka.batch_size
    esm_max_batching_window_seconds = aws_lambda_event_source_mapping.kafka.maximum_batching_window_in_seconds
    esm_source_access               = sort([for c in aws_lambda_event_source_mapping.kafka.source_access_configuration : "${c.type}=${c.uri}"])
    schedule_expression             = aws_scheduler_schedule.merge.schedule_expression
    function_object_version         = aws_lambda_function.writer.s3_object_version
  }
}
