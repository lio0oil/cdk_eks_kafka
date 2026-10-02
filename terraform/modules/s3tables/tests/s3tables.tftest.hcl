mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "111111111111"
    }
  }

  mock_resource "aws_kms_key" {
    defaults = {
      arn = "arn:aws:kms:ap-northeast-1:111111111111:key/00000000-0000-0000-0000-000000000000"
    }
  }

  mock_resource "aws_s3tables_table_bucket" {
    defaults = {
      arn = "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/kafka-events-dev"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::kafka-consumer-checkpoint-111111111111-dev"
    }
  }
}

mock_provider "awscc" {}

variables {
  table_bucket_name        = "kafka-events-dev"
  checkpoint_bucket_suffix = "dev"
  force_destroy            = true
  kms_key_alias            = "alias/kafka-consumer-s3tables-dev"
}

# table bucket と checkpoint バケットはどちらも EMR consumer のロールだけが書き込む同一の信頼境界の
# ため、鍵を分けずコストと運用をシンプルにする。書き込み元が異なるバケット（VPC Flow Logs 等）とは
# この鍵を共有しない。
run "one_rotating_kms_key_encrypts_the_table_bucket" {
  command = apply

  assert {
    condition     = aws_kms_key.s3tables.enable_key_rotation && aws_kms_alias.s3tables.name == var.kms_key_alias
    error_message = "KMS キーの自動ローテーション / エイリアスが期待と異なる"
  }

  assert {
    condition = (
      aws_s3tables_table_bucket.this.name == "kafka-events-dev"
      && aws_s3tables_table_bucket.this.force_destroy
      && aws_s3tables_table_bucket.this.encryption_configuration.sse_algorithm == "aws:kms"
      && aws_s3tables_table_bucket.this.encryption_configuration.kms_key_arn == aws_kms_key.s3tables.arn
    )
    error_message = "table bucket が KMS キーで暗号化されていない、または名前 / force_destroy が反映されていない"
  }
}

run "tables_live_in_the_events_namespace" {
  command = apply

  assert {
    condition     = aws_s3tables_namespace.events.namespace == "events"
    error_message = "namespace が events ではない"
  }
}

# Envelope 採用により Kafka に流れる proto は常に Envelope 1 種類。rawdata に Envelope の bytes を
# そのまま保存し、extra_type には oneof case 名を入れる。
run "event_table_stores_the_raw_envelope" {
  command = apply

  assert {
    condition = (
      awscc_s3tables_table.event.table_name == "sample_events_event"
      && awscc_s3tables_table.event.open_table_format == "ICEBERG"
      && awscc_s3tables_table.event.namespace == "events"
    )
    error_message = "event テーブルの名前 / 形式 / namespace が期待と異なる"
  }

  assert {
    condition = jsonencode(awscc_s3tables_table.event.iceberg_metadata.iceberg_schema.schema_field_list) == jsonencode([
      { id = 1, name = "event_id", type = "long", required = true },
      { id = 2, name = "event_datetime", type = "timestamp", required = true },
      { id = 3, name = "extra_type", type = "string", required = true },
      { id = 4, name = "rawdata", type = "binary", required = true },
    ])
    error_message = "event テーブルの列が event_id / event_datetime / extra_type / rawdata になっていない"
  }

  # 先頭は低カーディナリティで等値フィルタに使う identity(extra_type)、続けて day(event_datetime) で
  # 時系列の prune を効かせる
  assert {
    condition = jsonencode(awscc_s3tables_table.event.iceberg_metadata.iceberg_partition_spec.fields) == jsonencode([
      { source_id = 3, transform = "identity", name = "extra_type", field_id = 1000 },
      { source_id = 2, transform = "day", name = "event_datetime_day", field_id = 1001 },
    ])
    error_message = "event テーブルの partition が identity(extra_type) -> day(event_datetime) になっていない"
  }

  assert {
    condition = jsonencode(awscc_s3tables_table.event.iceberg_metadata.table_properties) == jsonencode({
      "write.delete.mode" = "copy-on-write"
      "write.merge.mode"  = "copy-on-write"
      "write.update.mode" = "copy-on-write"
    })
    error_message = "event テーブルの write モードが copy-on-write になっていない"
  }
}

# ProtoBuf のデシリアライズに失敗したデータの退避先（Dead Letter Queue）。全 schema 共通の 1 テーブル。
run "dlq_table_keeps_failed_rows_with_a_reason" {
  command = apply

  assert {
    condition = (
      awscc_s3tables_table.dlq.table_name == "sample_events_dlq"
      && awscc_s3tables_table.dlq.open_table_format == "ICEBERG"
      && awscc_s3tables_table.dlq.namespace == "events"
    )
    error_message = "dlq テーブルの名前 / 形式 / namespace が期待と異なる"
  }

  # schema_name は header 由来で、header に proto-schema が無い失敗（missing_schema）では NULL になるため任意
  assert {
    condition = jsonencode(awscc_s3tables_table.dlq.iceberg_metadata.iceberg_schema.schema_field_list) == jsonencode([
      { id = 1, name = "failed_at", type = "timestamp", required = true },
      { id = 2, name = "schema_name", type = "string", required = false },
      { id = 3, name = "rawdata", type = "binary", required = true },
      { id = 4, name = "reason", type = "string", required = true },
    ])
    error_message = "dlq テーブルの列が failed_at / schema_name / rawdata / reason になっていない"
  }

  assert {
    condition = jsonencode(awscc_s3tables_table.dlq.iceberg_metadata.iceberg_partition_spec.fields) == jsonencode([
      { source_id = 2, transform = "identity", name = "schema_name", field_id = 1000 },
      { source_id = 1, transform = "day", name = "failed_at_day", field_id = 1001 },
    ])
    error_message = "dlq テーブルの partition が identity(schema_name) -> day(failed_at) になっていない"
  }

  assert {
    condition = jsonencode(awscc_s3tables_table.dlq.iceberg_metadata.table_properties) == jsonencode({
      "write.delete.mode" = "copy-on-write"
      "write.merge.mode"  = "copy-on-write"
      "write.update.mode" = "copy-on-write"
    })
    error_message = "dlq テーブルの write モードが copy-on-write になっていない"
  }
}

run "tables_are_created_in_the_table_bucket" {
  command = apply

  assert {
    condition = (
      awscc_s3tables_table.event.table_bucket_arn == aws_s3tables_table_bucket.this.arn
      && awscc_s3tables_table.dlq.table_bucket_arn == aws_s3tables_table_bucket.this.arn
    )
    error_message = "テーブルが table bucket に作られていない"
  }
}

# 各 StreamingQuery は checkpoint バケットの <schema_name>/ 以下を使う（consumer/constants.py の checkpoint_for と一致）
run "checkpoint_bucket_name_is_unique_per_account_and_environment" {
  command = apply

  assert {
    condition     = output.checkpoint_bucket_name == "kafka-consumer-checkpoint-111111111111-dev"
    error_message = "checkpoint バケット名が kafka-consumer-checkpoint-<account>-<suffix> ではない"
  }
}

run "outputs_expose_what_the_consumer_needs" {
  command = apply

  assert {
    condition = (
      output.table_bucket_arn == aws_s3tables_table_bucket.this.arn
      && output.namespace_name == "events"
      && output.event_table_name == "sample_events_event"
      && output.dlq_table_name == "sample_events_dlq"
    )
    error_message = "consumer が使う ARN / namespace / テーブル名が出力されていない"
  }
}
