# table bucket のデータを復号する鍵のため、誤って削除するとデータが読めなくなる
resource "aws_kms_key" "s3tables" {
  description         = "kafka consumer の S3 Tables と checkpoint バケットの暗号化（checkpoint バケットは infra が同じ鍵で作る）"
  enable_key_rotation = true

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "s3tables" {
  name          = var.kms_key_alias
  target_key_id = aws_kms_key.s3tables.key_id
}

# テーブルは Terraform では管理しない。スキーマ進化（ALTER TABLE）と Terraform の定義がずれ、定義を直すと
# テーブルの置換（データ消失）になるため、scripts/s3tables が作る。
resource "aws_s3tables_table_bucket" "this" {
  name          = var.table_bucket_name
  force_destroy = var.force_destroy

  encryption_configuration = {
    sse_algorithm = "aws:kms"
    kms_key_arn   = aws_kms_key.s3tables.arn
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3tables_namespace" "events" {
  namespace        = "events"
  table_bucket_arn = aws_s3tables_table_bucket.this.arn

  lifecycle {
    prevent_destroy = true
  }
}
