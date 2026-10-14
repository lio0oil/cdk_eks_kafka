mock_provider "aws" {
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
}

variables {
  table_bucket_name = "kafka-events-dev"
  force_destroy     = true
  kms_key_alias     = "alias/kafka-consumer-s3tables-dev"
}

# table bucket と checkpoint バケット（infra が作る）はどちらも EMR consumer のロールだけが書き込む同一の
# 信頼境界のため、鍵を分けずコストと運用をシンプルにする。書き込み元が異なるバケット（VPC Flow Logs 等）
# とはこの鍵を共有しない。
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

run "events_namespace_is_created_in_the_table_bucket" {
  command = apply

  assert {
    condition = (
      aws_s3tables_namespace.events.namespace == "events"
      && aws_s3tables_namespace.events.table_bucket_arn == aws_s3tables_table_bucket.this.arn
    )
    error_message = "namespace が table bucket 内の events ではない"
  }
}

# テーブルは Terraform の管理外（scripts/s3tables が作る）。そのスクリプトと consumer が使う値を出力する
run "outputs_expose_what_the_table_script_and_consumer_need" {
  command = apply

  assert {
    condition = (
      output.table_bucket_arn == aws_s3tables_table_bucket.this.arn
      && output.kms_key_arn == aws_kms_key.s3tables.arn
      && output.namespace_name == "events"
    )
    error_message = "テーブル作成スクリプト・checkpoint バケット・consumer が使う ARN / namespace が出力されていない"
  }
}
