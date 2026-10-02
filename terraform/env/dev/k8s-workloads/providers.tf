# Kubernetes provider は、infra が作成したクラスターへ `aws eks get-token` で認証して接続する。
# 認証は AWS_PROFILE などの環境変数を引き継ぐ。接続先は infra の state から読むため plan 時点で確定している。
# この層は AWS のリソースを持たないため aws provider は使わない。
locals {
  eks_exec_args = ["eks", "get-token", "--cluster-name", local.infra.cluster_name, "--region", var.region]
}

provider "kubernetes" {
  host                   = local.infra.cluster_endpoint
  cluster_ca_certificate = base64decode(local.infra.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = local.eks_exec_args
  }
}
