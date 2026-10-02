# infra（VPC / EKS / NLB など）の出力を受け取る。broker 数やクラスター名のように 複数の root で
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

# external listener の名前・port と brokers[] の注入設定を kafka-cluster.yaml から導出する。
# infra の NLB と同じ入力（manifest と broker_count）から作るため、port の採番が一致する。
module "kafka_config" {
  source = "../../../modules/kafka-config"

  manifest_dir    = "${local.manifests_dir}/kafka"
  broker_count    = local.infra.broker_count
  advertised_host = local.infra.kafka_private_dns_name
}

# infra の適用（nodegroup と、coredns / eks-pod-identity-agent を含む全 Managed Addon）が完了してから
# 実行する。Pod は名前解決（coredns）と Pod Identity のクレデンシャル取得（agent）を要するため。
module "addons" {
  source = "../../../modules/addons"

  cluster_name          = local.infra.cluster_name
  region                = var.region
  vpc_id                = local.infra.vpc_id
  strimzi_version       = var.strimzi_version
  strimzi_chart_repo    = var.strimzi_chart_repo
  aws_lbc_chart_version = var.aws_lbc_chart_version
  aws_lbc_chart_repo    = var.aws_lbc_chart_repo

  strimzi_replicas             = var.strimzi_replicas
  strimzi_log_level            = var.strimzi_log_level
  aws_lbc_replica_count        = var.aws_lbc_replica_count
  aws_lbc_wait_timeout_seconds = var.aws_lbc_wait_timeout_seconds
  addons_manifest_dir          = "${local.manifests_dir}/addons"
  kafka_manifest_dir           = "${local.manifests_dir}/kafka"
}

# KafkaNodePool の CRD は Strimzi chart、TargetGroupBinding の CRD と mutating webhook は AWS LBC が
# 提供する。いずれも addons module の chart（wait=true）の Ready を待ってから apply する。
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

  depends_on = [module.addons]
}

# gp3 StorageClass（Prometheus / Alertmanager の PVC が要求）、kafka / strimzi-system の Namespace と
# Strimzi の CRD（PodMonitor の対象 Namespace、kube-state-metrics の ClusterRole）は addons module が用意する。
module "monitoring" {
  source = "../../../modules/monitoring"

  cluster_name                          = local.infra.cluster_name
  region                                = var.region
  manifest_dir                          = "${local.manifests_dir}/monitoring"
  kube_prometheus_stack_chart_version   = var.kube_prometheus_stack_chart_version
  kube_prometheus_stack_chart_repo      = var.kube_prometheus_stack_chart_repo
  fluent_bit_chart_version              = var.fluent_bit_chart_version
  fluent_bit_chart_repo                 = var.fluent_bit_chart_repo
  log_retention_days                    = var.log_retention_days
  retain_log_groups                     = !var.log_force_destroy
  enable_alertmanager_sns_log_forwarder = var.enable_alertmanager_sns_log_forwarder
  prometheus_replicas                   = var.prometheus_replicas
  alertmanager_replicas                 = var.alertmanager_replicas
  prometheus_resources                  = var.prometheus_resources
  alertmanager_resources                = var.alertmanager_resources
  fluent_bit_resources                  = var.fluent_bit_resources

  kube_prometheus_stack_wait_timeout_seconds = var.kube_prometheus_stack_wait_timeout_seconds
  sns_log_forwarder_runtime                  = var.sns_log_forwarder_runtime
  sns_log_forwarder_timeout_seconds          = var.sns_log_forwarder_timeout_seconds

  depends_on = [module.addons]
}
