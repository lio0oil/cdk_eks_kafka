# EKS とは別の state にする。table bucket と KMS キーは stg/prd で保持すべきデータを持つため、
# 基盤（platform）の destroy / 作り直しに巻き込まないようにする。
module "s3tables" {
  source = "../../../modules/s3tables"

  table_bucket_name        = var.table_bucket_name
  checkpoint_bucket_suffix = var.checkpoint_bucket_suffix
  force_destroy            = var.force_destroy
  kms_key_alias            = var.kms_key_alias
}
