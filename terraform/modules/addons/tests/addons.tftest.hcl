mock_provider "aws" {
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/mock"
    }
  }
}

mock_provider "helm" {}
mock_provider "kubernetes" {}

variables {
  cluster_name          = "eks-cluster-dev"
  region                = "ap-northeast-1"
  vpc_id                = "vpc-0123456789abcdef0"
  strimzi_version       = "1.0.0"
  strimzi_chart_repo    = "https://strimzi.io/charts/"
  aws_lbc_chart_version = "3.3.0"
  aws_lbc_chart_repo    = "https://aws.github.io/eks-charts"
  addons_manifest_dir   = "../../../cdk/manifests/addons"
  kafka_manifest_dir    = "../../../cdk/manifests/kafka"

  strimzi_replicas             = 2
  strimzi_log_level            = "WARN"
  aws_lbc_replica_count        = 2
  aws_lbc_wait_timeout_seconds = 600
}

run "gp3_is_the_default_storage_class" {
  command = apply

  assert {
    condition = (
      kubernetes_manifest.gp3_storage_class.manifest.metadata.name == "gp3"
      && kubernetes_manifest.gp3_storage_class.manifest.metadata.annotations["storageclass.kubernetes.io/is-default-class"] == "true"
    )
    error_message = "gp3 が default StorageClass として適用されていない"
  }
}

run "kafka_namespace_exists_before_strimzi_watches_it" {
  command = apply

  assert {
    condition     = kubernetes_manifest.kafka_namespace.manifest.metadata.name == "kafka"
    error_message = "kafka Namespace が適用されていない"
  }
}

run "strimzi_operator_is_installed_with_ha_settings" {
  command = apply

  assert {
    condition = (
      helm_release.strimzi.chart == "strimzi-kafka-operator"
      && helm_release.strimzi.repository == "https://strimzi.io/charts/"
      && helm_release.strimzi.version == "1.0.0"
      && helm_release.strimzi.namespace == "strimzi-system"
      && helm_release.strimzi.create_namespace
    )
    error_message = "Strimzi の chart / リポジトリ / バージョン / namespace が期待と異なる"
  }

  assert {
    condition = (
      yamldecode(helm_release.strimzi.values[0]).watchNamespaces == ["kafka"]
      && yamldecode(helm_release.strimzi.values[0]).replicas == 2
      && yamldecode(helm_release.strimzi.values[0]).logLevel == "WARN"
    )
    error_message = "Strimzi が kafka namespace を監視する 2 replica 構成になっていない"
  }

  # replicas: 2 の leader-election 構成で PDB が無いと、ノードドレインで 2 Pod が同時に evict されうる
  assert {
    condition     = yamldecode(helm_release.strimzi.values[0]).podDisruptionBudget.enabled
    error_message = "Strimzi の PodDisruptionBudget が有効になっていない"
  }

  # DoNotSchedule にすると Node 不足時に Pod が Pending になり Operator が止まる
  assert {
    condition = (
      yamldecode(helm_release.strimzi.values[0]).topologySpreadConstraints[0].whenUnsatisfiable == "ScheduleAnyway"
      && yamldecode(helm_release.strimzi.values[0]).topologySpreadConstraints[0].topologyKey == "topology.kubernetes.io/zone"
    )
    error_message = "Strimzi が AZ 分散を best-effort（ScheduleAnyway）で要求していない"
  }
}

run "aws_lbc_runs_with_the_pod_identity_service_account" {
  command = apply

  assert {
    condition = (
      kubernetes_service_account_v1.aws_lbc.metadata[0].name == "aws-load-balancer-controller"
      && kubernetes_service_account_v1.aws_lbc.metadata[0].namespace == "kube-system"
    )
    error_message = "AWS LBC 用の ServiceAccount が期待と異なる"
  }

  assert {
    condition = (
      helm_release.aws_lbc.chart == "aws-load-balancer-controller"
      && helm_release.aws_lbc.repository == "https://aws.github.io/eks-charts"
      && helm_release.aws_lbc.version == "3.3.0"
      && helm_release.aws_lbc.namespace == "kube-system"
    )
    error_message = "AWS LBC の chart / リポジトリ / バージョン / namespace が期待と異なる"
  }

  assert {
    condition = (
      yamldecode(helm_release.aws_lbc.values[0]).clusterName == "eks-cluster-dev"
      && yamldecode(helm_release.aws_lbc.values[0]).region == "ap-northeast-1"
      && yamldecode(helm_release.aws_lbc.values[0]).vpcId == "vpc-0123456789abcdef0"
      && yamldecode(helm_release.aws_lbc.values[0]).serviceAccount.create == false
      && yamldecode(helm_release.aws_lbc.values[0]).serviceAccount.name == "aws-load-balancer-controller"
    )
    error_message = "AWS LBC に渡す clusterName / region / vpcId / ServiceAccount が期待と異なる"
  }

  assert {
    condition = (
      yamldecode(helm_release.aws_lbc.values[0]).replicaCount == 2
      && yamldecode(helm_release.aws_lbc.values[0]).podDisruptionBudget.maxUnavailable == 1
    )
    error_message = "AWS LBC が 2 replica / maxUnavailable=1 の PDB になっていない"
  }

  # TargetGroupBinding の apply 時に LBC の MutatingWebhook が呼ばれるため、Pod が Ready に
  # なるまで待たないと webhook の endpoint が無く apply が失敗する
  assert {
    condition     = helm_release.aws_lbc.wait && helm_release.aws_lbc.timeout == 600
    error_message = "AWS LBC が Pod の Ready を 10 分まで待つ設定になっていない"
  }

  assert {
    condition     = module.lbc_pod_identity.role_arn != ""
    error_message = "AWS LBC の Pod Identity Role が作られていない"
  }
}

# Operator の replica 数・ログレベル・LBC の replica 数・Ready 待ちの上限は、module に値を持たず呼び出し側が決める
run "operator_settings_come_from_variables" {
  command = apply

  variables {
    strimzi_replicas             = 3
    strimzi_log_level            = "ERROR"
    aws_lbc_replica_count        = 4
    aws_lbc_wait_timeout_seconds = 900
  }

  assert {
    condition = (
      yamldecode(helm_release.strimzi.values[0]).replicas == 3
      && yamldecode(helm_release.strimzi.values[0]).logLevel == "ERROR"
    )
    error_message = "Strimzi の replicas / logLevel が variables から決まっていない"
  }

  assert {
    condition = (
      yamldecode(helm_release.aws_lbc.values[0]).replicaCount == 4
      && helm_release.aws_lbc.timeout == 900
    )
    error_message = "AWS LBC の replicaCount / Ready 待ちの上限が variables から決まっていない"
  }
}
