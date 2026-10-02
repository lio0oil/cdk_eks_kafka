data "aws_caller_identity" "current" {}

locals {
  # Iceberg の書き込みモードは copy-on-write。2 つのテーブルで共通
  table_properties = {
    "write.merge.mode"  = "copy-on-write"
    "write.update.mode" = "copy-on-write"
    "write.delete.mode" = "copy-on-write"
  }
}

resource "aws_kms_key" "s3tables" {
  description         = "kafka consumer の S3 Tables と checkpoint バケットの暗号化"
  enable_key_rotation = true
}

resource "aws_kms_alias" "s3tables" {
  name          = var.kms_key_alias
  target_key_id = aws_kms_key.s3tables.key_id
}

resource "aws_s3tables_table_bucket" "this" {
  name          = var.table_bucket_name
  force_destroy = var.force_destroy

  encryption_configuration = {
    sse_algorithm = "aws:kms"
    kms_key_arn   = aws_kms_key.s3tables.arn
  }
}

resource "aws_s3tables_namespace" "events" {
  namespace        = "events"
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
}

# Envelope 構造の採用により、Kafka に流れる proto は常に Envelope 1 種類。consumer は Envelope.event の
# id / datetime を flatten し、oneof case 名を extra_type に書き込み、元の Envelope の bytes を rawdata に
# そのまま保存する（後段の分析や再処理は events.desc で再 deserialize する）。
# 新しい extra 型を追加しても、列や consumer の変更は不要（extra_type の値が増えるだけ）。
resource "awscc_s3tables_table" "event" {
  table_bucket_arn  = aws_s3tables_table_bucket.this.arn
  namespace         = aws_s3tables_namespace.events.namespace
  table_name        = "sample_events_event"
  open_table_format = "ICEBERG"

  iceberg_metadata = {
    iceberg_schema = {
      schema_field_list = [
        { id = 1, name = "event_id", type = "long", required = true },
        { id = 2, name = "event_datetime", type = "timestamp", required = true },
        { id = 3, name = "extra_type", type = "string", required = true },
        { id = 4, name = "rawdata", type = "binary", required = true },
      ]
    }

    # 先頭は低カーディナリティで等値フィルタに使う identity(extra_type)。続けて day(event_datetime)
    # で時系列の prune を効かせる。
    iceberg_partition_spec = {
      fields = [
        { source_id = 3, transform = "identity", name = "extra_type", field_id = 1000 },
        { source_id = 2, transform = "day", name = "event_datetime_day", field_id = 1001 },
      ]
    }

    table_properties = local.table_properties
  }
}

# ProtoBuf のデシリアライズに失敗したデータの退避先（Dead Letter Queue）。全 schema 共通の 1 テーブル。
# 失敗頻度は低い前提で、並行して書いても Iceberg の楽観ロックの retry は許容範囲。
resource "awscc_s3tables_table" "dlq" {
  table_bucket_arn  = aws_s3tables_table_bucket.this.arn
  namespace         = aws_s3tables_namespace.events.namespace
  table_name        = "sample_events_dlq"
  open_table_format = "ICEBERG"

  iceberg_metadata = {
    iceberg_schema = {
      schema_field_list = [
        { id = 1, name = "failed_at", type = "timestamp", required = true },
        # header 由来の proto-schema をそのまま入れる。header に proto-schema が無い失敗
        # （missing_schema）では NULL になるため任意
        { id = 2, name = "schema_name", type = "string", required = false },
        { id = 3, name = "rawdata", type = "binary", required = true },
        { id = 4, name = "reason", type = "string", required = true },
      ]
    }

    # 先頭は schema 別の集計を等値 prune できる identity(schema_name)。続けて day(failed_at) で
    # 時系列の prune（アラート / lifecycle）を効かせる。
    iceberg_partition_spec = {
      fields = [
        { source_id = 2, transform = "identity", name = "schema_name", field_id = 1000 },
        { source_id = 1, transform = "day", name = "failed_at_day", field_id = 1001 },
      ]
    }

    table_properties = local.table_properties
  }
}

# consumer の writeStream.option("checkpointLocation", ...) 用バケット。各 StreamingQuery は
# <schema_name>/ のようにプレフィックスで分割して使う。
module "checkpoint_bucket" {
  source = "../secure-bucket"

  bucket_name   = "kafka-consumer-checkpoint-${data.aws_caller_identity.current.account_id}-${var.checkpoint_bucket_suffix}"
  force_destroy = var.force_destroy
  kms_key_arn   = aws_kms_key.s3tables.arn
}
