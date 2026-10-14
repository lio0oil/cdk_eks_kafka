# infra（VPC / EKS / NLB / ESM 用の SG とバケットなど）の出力を受け取る。
# infra 側の backend（backend.tf）と同じバケット・キーを指す。
data "terraform_remote_state" "infra" {
  backend = "s3"

  config = {
    bucket = "ekscdk-tfstate-9025850b"
    key    = "dev/infra.tfstate"
    region = "ap-northeast-1"
  }
}

# topic 名は、topic を作る k8s-workloads 層の出力を単一の真実の源にする（manifest を二重に読まない）。
data "terraform_remote_state" "k8s_workloads" {
  backend = "s3"

  config = {
    bucket = "ekscdk-tfstate-9025850b"
    key    = "dev/k8s-workloads.tfstate"
    region = "ap-northeast-1"
  }
}

locals {
  infra         = data.terraform_remote_state.infra.outputs
  k8s_workloads = data.terraform_remote_state.k8s_workloads.outputs
}

# 適用の順序は infra -> k8s-addons -> k8s-workloads -> app。topic 名は k8s-workloads の state から読むため、
# その適用が済んでいないと plan が止まる。ESM のポーラーは Kafka broker への到達性（k8s-workloads 層が
# NLB の TargetGroup に broker を登録すること）も前提だが、これは値の受け渡しでは表せないため、
# この順に適用する運用とする。
module "consumer_app" {
  source = "../../../modules/consumer-app"

  cluster_name          = local.infra.cluster_name
  subnet_ids            = local.infra.kafka_subnet_ids
  esm_security_group_id = local.infra.esm_security_group_id
  data_bucket_arn       = local.infra.data_bucket_arn
  artifact_bucket_name  = local.infra.kafka_consumer_artifact_bucket_name
  code_object_version   = var.kafka_consumer_code_object_version
  bootstrap_dns_name    = local.infra.kafka_private_dns_name
  bootstrap_port        = local.infra.kafka_external_listener_port
  topic_name            = local.k8s_workloads.test_topic_name
  table_bucket_arn      = local.infra.table_bucket_arn
  log_retention_days    = var.log_retention_days
  force_destroy         = var.log_force_destroy

  lambda_runtime                  = var.lambda_runtime
  lambda_timeout_seconds          = var.lambda_timeout_seconds
  lambda_application_log_level    = var.lambda_application_log_level
  esm_starting_position           = var.esm_starting_position
  esm_batch_size                  = var.esm_batch_size
  esm_max_batching_window_seconds = var.esm_max_batching_window_seconds
}
