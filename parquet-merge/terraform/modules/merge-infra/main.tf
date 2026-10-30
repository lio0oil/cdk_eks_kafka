# staging は Lambda が書く Parquet と MERGE ジョブの状態、artifact は Lambda のコードと PySpark スクリプトを置く。
# 暗号化・public access 遮断・TLS 必須の設定は両バケットで同じなので、for_each で 1 つの定義を共有する。
locals {
  buckets = {
    staging  = { versioning = false }
    artifact = { versioning = true }
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket_prefix = "${var.name_prefix}-${each.key}-"
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.buckets

  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lambda は VersionId を明示参照してコードを読む。S3Key だけではコードの変更を検知できない。
resource "aws_s3_bucket_versioning" "this" {
  for_each = { for k, v in local.buckets : k => v if v.versioning }

  bucket = aws_s3_bucket.this[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_policy" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.this[each.key].arn, "${aws_s3_bucket.this[each.key].arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  # public access block と同時に適用すると競合する（OperationAborted）ため、先に確定させる
  depends_on = [aws_s3_bucket_public_access_block.this]
}

# state/ には MERGE ジョブの watermark を置くので、期限を付けるルールは incoming/ と dlq/ に限る。
resource "aws_s3_bucket_lifecycle_configuration" "staging" {
  bucket = aws_s3_bucket.this["staging"].id

  dynamic "rule" {
    for_each = toset(["incoming/", "dlq/"])

    content {
      id     = "expire-${trimsuffix(rule.value, "/")}"
      status = "Enabled"

      filter {
        prefix = rule.value
      }

      expiration {
        days = var.staging_retention_days
      }
    }
  }
}

# 事前初期化キャパシティ（initial_capacity）は持たない。持つと停止中も費用がかかる。
resource "aws_emrserverless_application" "this" {
  name          = var.name_prefix
  release_label = var.release_label
  type          = "SPARK"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled = true
  }

  # MERGE が並行すると、同じメッセージが両方の実行で「本テーブルに無い」と判断され重複して入る
  scheduler_configuration {
    max_concurrent_runs   = 1
    queue_timeout_minutes = var.queue_timeout_minutes
  }
}
