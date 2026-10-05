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

  manifests_dir = "${path.module}/../../../manifests"
}

# infra の適用（nodegroup と、coredns / eks-pod-identity-agent を含む全 Managed Addon）が完了してから
# 実行する。Pod は名前解決（coredns）と Pod Identity のクレデンシャル取得（agent）を要するため。
# この層は CRD を提供する Helm（Strimzi・AWS LBC・kube-prometheus-stack）までを管理し、CRD に依存する
# manifest（Kafka CR・TargetGroupBinding・PodMonitor など）は次の k8s-workloads 層が管理する。
# kubernetes_manifest は plan 時に CRD のスキーマを要求するため、CRD を作る層と使う層を分けている。
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

# gp3 StorageClass（Prometheus / Alertmanager の PVC が要求）は addons module が用意する。
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
