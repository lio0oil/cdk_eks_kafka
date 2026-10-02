# tfvars が module に渡ること、各 provider の組み合わせで apply が通ることを mock で検証する。
# command = apply は mock provider に対する適用で、実 AWS には触れない。

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
      arn = "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/mock-table-bucket"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::kafka-consumer-checkpoint-111111111111-dev"
    }
  }
}

mock_provider "awscc" {}

run "dev_tfvars_flow_into_the_module" {
  command = apply

  assert {
    condition     = output.checkpoint_bucket_name == "kafka-consumer-checkpoint-111111111111-${var.checkpoint_bucket_suffix}"
    error_message = "tfvars の checkpoint_bucket_suffix が module に渡っていない"
  }

  assert {
    condition     = output.namespace_name == "events" && output.event_table_name == "sample_events_event"
    error_message = "namespace / テーブル名が出力されていない"
  }
}
