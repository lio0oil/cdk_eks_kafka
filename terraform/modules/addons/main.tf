# Kafka broker/controller と Prometheus/Alertmanager が共有する default StorageClass。
# Kafka 専用の StorageClass（gp3-kafka）は単一消費者のため kafka module 側で管理する。
resource "kubectl_manifest" "gp3_storage_class" {
  yaml_body = file("${var.addons_manifest_dir}/gp3-storageclass.yaml")
}

# Strimzi chart に watchNamespaces: ["kafka"] を渡すため、対象 namespace が事前に存在しないと
# RoleBinding の作成時に Helm install が失敗する（namespaces "kafka" not found）。
resource "kubectl_manifest" "kafka_namespace" {
  yaml_body = file("${var.kafka_manifest_dir}/namespace.yaml")
}

resource "helm_release" "strimzi" {
  name             = "strimzi-operator"
  chart            = "strimzi-kafka-operator"
  repository       = var.strimzi_chart_repo
  version          = var.strimzi_version
  namespace        = "strimzi-system"
  create_namespace = true

  values = [yamlencode({
    watchNamespaces = ["kafka"]
    replicas        = var.strimzi_replicas
    logLevel        = var.strimzi_log_level
    # toleration は不要: system nodegroup には taint が無く、kafka nodegroup の DedicatedKafka taint を
    # tolerate しないため自然に system に schedule される。
    # ScheduleAnyway は best-effort（AZ 分散を優先するが満たせなくても schedule する）。
    # DoNotSchedule にすると node 障害で Pod が Pending に陥るリスクを取ることになる。
    topologySpreadConstraints = [{
      maxSkew           = 1
      topologyKey       = "topology.kubernetes.io/zone"
      whenUnsatisfiable = "ScheduleAnyway"
      labelSelector     = { matchLabels = { name = "strimzi-cluster-operator" } }
    }]
    # chart 既定は無効。replicas: 2 の leader-election 構成だが PDB が無いままノードドレインが
    # 走ると 2 Pod が同時に evict されうるため有効化する。値は chart 既定の minAvailable: 1 に乗る。
    podDisruptionBudget = { enabled = true }
  })]

  depends_on = [kubectl_manifest.kafka_namespace]
}

# Strimzi の per-broker Service を NLB の TargetGroup に TargetGroupBinding 経由で動的にバインドする。
# ASG ベースの static attachment と異なり、Pod のローリング更新時も Service Endpoints との同期が追従する。
module "lbc_pod_identity" {
  source = "../pod-identity"

  cluster_name      = var.cluster_name
  role_name         = "aws-lbc-pod-identity-${var.cluster_name}"
  namespace         = "kube-system"
  service_account   = "aws-load-balancer-controller"
  policy_statements = jsondecode(file("${var.addons_manifest_dir}/aws-lbc-iam-policy.json")).Statement
}

# chart 側で ServiceAccount を作らない（serviceAccount.create=false）ため、Pod Identity の紐付け先として
# ここで作っておく。
resource "kubernetes_service_account_v1" "aws_lbc" {
  metadata {
    name      = "aws-load-balancer-controller"
    namespace = "kube-system"
  }
}

resource "helm_release" "aws_lbc" {
  name       = "aws-load-balancer-controller"
  chart      = "aws-load-balancer-controller"
  repository = var.aws_lbc_chart_repo
  version    = var.aws_lbc_chart_version
  namespace  = "kube-system"

  # TargetGroupBinding の apply 時に LBC の MutatingWebhook が呼ばれるため、webhook Service の
  # endpoint が立ち上がるまで（Pod が Ready になるまで）待つ。
  wait    = true
  timeout = var.aws_lbc_wait_timeout_seconds

  values = [yamlencode({
    clusterName = var.cluster_name
    region      = var.region
    vpcId       = var.vpc_id
    serviceAccount = {
      create = false
      name   = "aws-load-balancer-controller"
    }
    # toleration は不要: DedicatedKafka taint で kafka nodegroup から弾かれ、system に自然に乗る
    replicaCount = var.aws_lbc_replica_count
    # chart 既定の configureDefaultAffinity=true で「同 node に co-locate しない」podAntiAffinity が
    # 入る。それに加えて AZ 分散を topologySpreadConstraints で上乗せする。
    topologySpreadConstraints = [{
      maxSkew           = 1
      topologyKey       = "topology.kubernetes.io/zone"
      whenUnsatisfiable = "ScheduleAnyway"
      labelSelector     = { matchLabels = { "app.kubernetes.io/name" = "aws-load-balancer-controller" } }
    }]
    # chart 既定は空 {} で、PDB が生成されない。MutatingWebhook が止まると TargetGroupBinding 等の
    # apply が詰まるため、replicaCount を増やしても同時喪失を 1 に固定できる maxUnavailable で表す
    # （minAvailable: 1 だと replicaCount が増えるほど許容喪失が増えてしまう）。
    podDisruptionBudget = { maxUnavailable = 1 }
  })]

  depends_on = [
    kubernetes_service_account_v1.aws_lbc,
    module.lbc_pod_identity,
  ]
}
