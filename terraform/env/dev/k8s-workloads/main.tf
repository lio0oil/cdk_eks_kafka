# infra（VPC / EKS / NLB など）の出力を受け取る。broker 数や controller 数のように、複数の層で
# 食い違うと壊れる値は、tfvars に重ねて持たず infra の出力を単一の真実の源にする。
# infra 側の backend（backend.tf）と同じバケット・キーを指す。
data "terraform_remote_state" "infra" {
  backend = "s3"

  config = {
    bucket = "ekscdk-tfstate-9025850b"
    key    = "dev/infra.tfstate"
    region = "ap-northeast-1"
  }
}

locals {
  infra = data.terraform_remote_state.infra.outputs

  # 移行期間中は CDK 側の manifests/ を参照する。cdk/ を削除する時点で terraform/ 配下へ移す
  manifests_dir = "${path.module}/../../../../cdk/manifests"
}

# この層は CRD に依存する manifest（Kafka CR・TargetGroupBinding・PodMonitor など）を管理する。
# kubernetes_manifest は plan 時に CRD のスキーマを要求するため、k8s-addons 層（Strimzi・AWS LBC・
# kube-prometheus-stack の Helm が CRD を作る）の適用が完了してから実行する。

# external listener の名前・port と brokers[] の注入設定を kafka-cluster.yaml から導出する。
# infra の NLB と同じ入力（manifest と broker_count）から作るため、port の採番が一致する。
module "kafka_config" {
  source = "../../../modules/kafka-config"

  manifest_dir    = "${local.manifests_dir}/kafka"
  broker_count    = local.infra.broker_count
  advertised_host = local.infra.kafka_private_dns_name
}

module "kafka" {
  source = "../../../modules/kafka"

  kafka_manifest_dir      = "${local.manifests_dir}/kafka"
  monitoring_manifest_dir = "${local.manifests_dir}/monitoring"
  broker_count            = local.infra.broker_count
  controller_count        = local.infra.kafka_controller_count
  delete_claim            = var.kafka_delete_claim
  broker_resources        = var.kafka_broker_resources
  controller_resources    = var.kafka_controller_resources
  external_listener_name  = module.kafka_config.external_listener_name
  broker_configs          = module.kafka_config.broker_configs
  target_group_arns       = local.infra.kafka_target_group_arns
  target_port             = module.kafka_config.external_listener_port
  nlb_security_group_id   = local.infra.kafka_nlb_security_group_id
  vpc_id                  = local.infra.vpc_id
  topic_retention_ms      = var.kafka_topic_retention_ms
}

module "monitoring_resources" {
  source = "../../../modules/monitoring-resources"

  manifest_dir = "${local.manifests_dir}/monitoring"
}
