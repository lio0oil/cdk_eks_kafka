provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Kubernetes 系の provider は、infra が作成したクラスターへ `aws eks get-token` で認証して接続する。
# 認証は AWS_PROFILE などの環境変数を引き継ぐ。接続先は infra の state から読むため plan 時点で確定している
# （クラスターと同じ state に置くと、初回の plan で接続先が未確定になり、plan 時にクラスターへ接続する
# kubernetes_manifest が失敗する）。
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

provider "helm" {
  kubernetes = {
    host                   = local.infra.cluster_endpoint
    cluster_ca_certificate = base64decode(local.infra.cluster_certificate_authority_data)

    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = local.eks_exec_args
    }
  }
}
