mock_provider "aws" {
  mock_resource "aws_emrserverless_application" {
    defaults = {
      arn = "arn:aws:emr-serverless:ap-northeast-1:111111111111:/applications/00mockapplication"
    }
  }
}

# mock の既定値は全インスタンスで同じ ARN になり、バケットの取り違えを検出できない。
# staging と artifact に別の値を与える。
override_resource {
  target = aws_s3_bucket.this["staging"]
  values = {
    arn = "arn:aws:s3:::mock-staging"
  }
}

override_resource {
  target = aws_s3_bucket.this["artifact"]
  values = {
    arn = "arn:aws:s3:::mock-artifact"
  }
}

variables {
  name_prefix   = "parquet-merge-dev"
  force_destroy = true
}

run "buckets_are_named_by_prefix_and_destroyable_when_requested" {
  command = apply

  assert {
    condition = (
      aws_s3_bucket.this["staging"].bucket_prefix == "parquet-merge-dev-staging-"
      && aws_s3_bucket.this["artifact"].bucket_prefix == "parquet-merge-dev-artifact-"
      && aws_s3_bucket.this["staging"].force_destroy
      && aws_s3_bucket.this["artifact"].force_destroy
    )
    error_message = "バケットの接頭辞 / force_destroy が期待と異なる"
  }

  assert {
    condition     = length(aws_s3_bucket.this) == 2
    error_message = "バケットが staging と artifact の 2 つになっていない"
  }
}

run "buckets_block_public_access" {
  command = apply

  assert {
    condition = alltrue([
      for b in aws_s3_bucket_public_access_block.this :
      b.block_public_acls && b.block_public_policy && b.ignore_public_acls && b.restrict_public_buckets
    ]) && length(aws_s3_bucket_public_access_block.this) == 2
    error_message = "public access が全遮断になっていないバケットがある"
  }

  assert {
    condition = alltrue([
      for k, b in aws_s3_bucket_public_access_block.this : b.bucket == aws_s3_bucket.this[k].id
    ])
    error_message = "public access block が対応するバケットに付いていない"
  }
}

run "buckets_are_encrypted_with_sse_s3" {
  command = apply

  assert {
    condition = alltrue([
      for e in aws_s3_bucket_server_side_encryption_configuration.this :
      one([for r in e.rule : r.apply_server_side_encryption_by_default[0].sse_algorithm]) == "AES256"
    ]) && length(aws_s3_bucket_server_side_encryption_configuration.this) == 2
    error_message = "SSE-S3（AES256）になっていないバケットがある"
  }

  assert {
    condition = alltrue([
      for k, e in aws_s3_bucket_server_side_encryption_configuration.this : e.bucket == aws_s3_bucket.this[k].id
    ])
    error_message = "暗号化設定が対応するバケットに付いていない"
  }
}

run "buckets_deny_non_tls_access" {
  command = apply

  assert {
    condition = alltrue([
      for p in aws_s3_bucket_policy.this : (
        jsondecode(p.policy).Statement[0].Effect == "Deny"
        && jsondecode(p.policy).Statement[0].Principal == "*"
        && jsondecode(p.policy).Statement[0].Action == "s3:*"
        && jsondecode(p.policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false"
        && length(jsondecode(p.policy).Statement) == 1
      )
    ]) && length(aws_s3_bucket_policy.this) == 2
    error_message = "バケットとオブジェクトへの TLS 以外のアクセスを拒否していないバケットがある"
  }

  assert {
    condition = alltrue([
      for k, p in aws_s3_bucket_policy.this : (
        p.bucket == aws_s3_bucket.this[k].id
        && toset(jsondecode(p.policy).Statement[0].Resource) == toset([aws_s3_bucket.this[k].arn, "${aws_s3_bucket.this[k].arn}/*"])
      )
    ])
    error_message = "TLS 必須ポリシーが対応するバケットに付いていない、または別のバケットの ARN を指している"
  }
}

# 差し替えのたびに VersionId が変わり、Lambda はその VersionId を明示参照する
run "only_the_artifact_bucket_is_versioned" {
  command = apply

  assert {
    condition = (
      length(aws_s3_bucket_versioning.this) == 1
      && one(aws_s3_bucket_versioning.this["artifact"].versioning_configuration).status == "Enabled"
    )
    error_message = "バージョニングが artifact バケットだけで有効になっていない"
  }

  assert {
    condition     = aws_s3_bucket_versioning.this["artifact"].bucket == aws_s3_bucket.this["artifact"].id
    error_message = "バージョニングが artifact バケットに付いていない"
  }
}

run "staging_expires_incoming_and_dlq_after_14_days_and_keeps_state" {
  command = apply

  assert {
    condition = (
      toset([for r in aws_s3_bucket_lifecycle_configuration.staging.rule : one(r.filter).prefix])
      == toset(["incoming/", "dlq/"])
      && length(aws_s3_bucket_lifecycle_configuration.staging.rule) == 2
    )
    error_message = "ライフサイクルの対象が incoming/ と dlq/ の 2 つだけになっていない（state/ や bucket 全体を消してはならない）"
  }

  assert {
    condition = alltrue([
      for r in aws_s3_bucket_lifecycle_configuration.staging.rule :
      r.status == "Enabled" && one(r.expiration).days == 14
    ])
    error_message = "incoming/ と dlq/ のルールが有効かつ 14 日で削除になっていない"
  }

  assert {
    condition     = aws_s3_bucket_lifecycle_configuration.staging.bucket == aws_s3_bucket.this["staging"].id
    error_message = "ライフサイクルが staging バケットに付いていない"
  }
}

run "retention_days_can_be_changed" {
  command = apply

  variables {
    staging_retention_days = 30
  }

  assert {
    condition     = alltrue([for r in aws_s3_bucket_lifecycle_configuration.staging.rule : one(r.expiration).days == 30])
    error_message = "staging_retention_days がライフサイクルに反映されていない"
  }
}

run "emr_application_runs_one_job_at_a_time_and_stops_when_idle" {
  command = apply

  assert {
    condition = (
      aws_emrserverless_application.this.release_label == "emr-7.14.0"
      && aws_emrserverless_application.this.type == "SPARK"
      && aws_emrserverless_application.this.name == "parquet-merge-dev"
    )
    error_message = "EMR Serverless の release_label / type / name が期待と異なる"
  }

  assert {
    condition     = one(aws_emrserverless_application.this.scheduler_configuration).max_concurrent_runs == 1
    error_message = "maxConcurrentRuns が 1 でない（MERGE が並行すると重複排除が崩れる）"
  }

  assert {
    condition = (
      one(aws_emrserverless_application.this.auto_start_configuration).enabled
      && one(aws_emrserverless_application.this.auto_stop_configuration).enabled
    )
    error_message = "auto start / auto stop が有効でない"
  }

  assert {
    condition     = length(aws_emrserverless_application.this.initial_capacity) == 0
    error_message = "事前初期化キャパシティが設定されている（停止中の費用が発生する）"
  }
}

run "queue_timeout_defaults_to_the_minimum" {
  command = apply

  assert {
    condition     = one(aws_emrserverless_application.this.scheduler_configuration).queue_timeout_minutes == 15
    error_message = "queue_timeout_minutes の既定値が 15 でない"
  }
}

run "queue_timeout_and_release_label_can_be_changed" {
  command = apply

  variables {
    queue_timeout_minutes = 60
    release_label         = "emr-7.15.0"
  }

  assert {
    condition = (
      one(aws_emrserverless_application.this.scheduler_configuration).queue_timeout_minutes == 60
      && aws_emrserverless_application.this.release_label == "emr-7.15.0"
    )
    error_message = "queue_timeout_minutes / release_label が反映されていない"
  }
}

run "queue_timeout_below_the_api_minimum_is_rejected" {
  command = plan

  variables {
    queue_timeout_minutes = 5
  }

  expect_failures = [var.queue_timeout_minutes]
}

run "outputs_expose_the_buckets_and_the_application" {
  command = apply

  assert {
    condition = (
      output.staging_bucket_name == aws_s3_bucket.this["staging"].bucket
      && output.artifact_bucket_name == aws_s3_bucket.this["artifact"].bucket
    )
    error_message = "バケット名の出力が各バケットを指していない"
  }

  assert {
    condition = (
      output.staging_bucket_arn == "arn:aws:s3:::mock-staging"
      && output.artifact_bucket_arn == "arn:aws:s3:::mock-artifact"
    )
    error_message = "バケット ARN の出力が取り違えられている（staging と artifact が逆、または同じバケット）"
  }

  assert {
    condition = (
      output.emr_application_id == aws_emrserverless_application.this.id
      && output.emr_application_arn == aws_emrserverless_application.this.arn
    )
    error_message = "EMR Serverless アプリの出力がアプリを指していない"
  }
}
