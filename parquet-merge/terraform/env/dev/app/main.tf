# infra（バケットと EMR Serverless アプリ）の出力を受け取る。
# infra 側の backend（../infra/backend.tf）と同じバケット・キーを指す。
data "terraform_remote_state" "infra" {
  backend = "s3"

  config = {
    bucket = "ekscdk-tfstate-9025850b"
    key    = "dev/parquet-merge-infra.tfstate"
    region = "ap-northeast-1"
  }
}

locals {
  infra = data.terraform_remote_state.infra.outputs
}

# 適用の順序は infra -> function.zip と MERGE ジョブのスクリプトのアップロード -> app。
# アップロードで得た VersionId を function_object_version に書いて適用する。
module "merge_app" {
  source = "../../../modules/merge-app"

  name_prefix = var.name_prefix

  subnet_ids            = var.subnet_ids
  esm_security_group_id = var.esm_security_group_id
  bootstrap_dns_name    = var.bootstrap_dns_name
  bootstrap_port        = var.bootstrap_port
  topic_name            = var.topic_name

  table_bucket_arn = var.table_bucket_arn
  namespace_name   = var.namespace_name
  table_name       = var.table_name

  staging_bucket_name  = local.infra.staging_bucket_name
  staging_bucket_arn   = local.infra.staging_bucket_arn
  artifact_bucket_name = local.infra.artifact_bucket_name
  artifact_bucket_arn  = local.infra.artifact_bucket_arn
  emr_application_id   = local.infra.emr_application_id
  emr_application_arn  = local.infra.emr_application_arn

  s3_object_version = var.function_object_version

  lambda_timeout_seconds          = var.lambda_timeout_seconds
  lambda_memory_mb                = var.lambda_memory_mb
  log_retention_days              = var.log_retention_days
  esm_starting_position           = var.esm_starting_position
  esm_batch_size                  = var.esm_batch_size
  esm_max_batching_window_seconds = var.esm_max_batching_window_seconds

  schedule_expression     = var.schedule_expression
  merge_margin_seconds    = var.merge_margin_seconds
  spark_submit_parameters = var.spark_submit_parameters
}
