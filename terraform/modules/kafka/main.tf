locals {
  # マニフェストの <PLACEHOLDER> を ${PLACEHOLDER} に変換して templatestring で置換する。
  # 値が渡されていないプレースホルダーは templatestring がエラーにするため、置換漏れが黙って残らない。
  # Grafana dashboard など ${ を含むファイルには使わない。
  placeholder_pattern     = "/<([A-Z][A-Z0-9_]*)>/"
  placeholder_replacement = "$$${$1}"

  # templatestring の第 1 引数は式ではなく文字列への直接参照でなければならないため、変換後の文字列を local に置く
  controller_pool_template      = replace(file("${var.kafka_manifest_dir}/node-pool-controller.yaml"), local.placeholder_pattern, local.placeholder_replacement)
  broker_pool_template          = replace(file("${var.kafka_manifest_dir}/node-pool-broker.yaml"), local.placeholder_pattern, local.placeholder_replacement)
  target_group_binding_template = replace(file("${var.kafka_manifest_dir}/target-group-binding.yaml"), local.placeholder_pattern, local.placeholder_replacement)
  test_topic_template           = replace(file("${var.kafka_manifest_dir}/topics/test-topic.yaml"), local.placeholder_pattern, local.placeholder_replacement)

  node_pool_template_vars = {
    DELETE_CLAIM = tostring(var.delete_claim)
  }

  controller_pool_yaml = templatestring(
    local.controller_pool_template,
    merge(local.node_pool_template_vars, {
      CONTROLLER_REPLICAS       = tostring(var.controller_count)
      CONTROLLER_MEMORY_REQUEST = var.controller_resources.memory_request
      CONTROLLER_MEMORY_LIMIT   = var.controller_resources.memory_limit
      CONTROLLER_CPU_REQUEST    = var.controller_resources.cpu_request
      CONTROLLER_CPU_LIMIT      = var.controller_resources.cpu_limit
      CONTROLLER_STORAGE_SIZE   = var.controller_resources.storage_size
      CONTROLLER_JVM_XMS        = var.controller_resources.jvm_xms
      CONTROLLER_JVM_XMX        = var.controller_resources.jvm_xmx
    }),
  )

  broker_pool_yaml = templatestring(
    local.broker_pool_template,
    merge(local.node_pool_template_vars, {
      BROKER_REPLICAS       = tostring(var.broker_count)
      BROKER_MEMORY_REQUEST = var.broker_resources.memory_request
      BROKER_MEMORY_LIMIT   = var.broker_resources.memory_limit
      BROKER_CPU_REQUEST    = var.broker_resources.cpu_request
      BROKER_CPU_LIMIT      = var.broker_resources.cpu_limit
      BROKER_STORAGE_SIZE   = var.broker_resources.storage_size
      BROKER_JVM_XMS        = var.broker_resources.jvm_xms
      BROKER_JVM_XMX        = var.broker_resources.jvm_xmx
    }),
  )

  # kafka-cluster.yaml の external listener には brokers[] を書かず、broker_count から生成した
  # ものをここで注入する（broker_count を単一の真実の源にする）。
  kafka_manifest = yamldecode(file("${var.kafka_manifest_dir}/kafka-cluster.yaml"))

  kafka_cluster = merge(local.kafka_manifest, {
    spec = merge(local.kafka_manifest.spec, {
      kafka = merge(local.kafka_manifest.spec.kafka, {
        # 条件式の両辺は同じ型でなければならず、brokers を持つ listener と持たない listener で
        # 型が異なるため、JSON を経由して型を揃える
        listeners = [
          for l in local.kafka_manifest.spec.kafka.listeners :
          jsondecode(l.name == var.external_listener_name ? jsonencode(merge(l, { configuration = merge(l.configuration, { brokers = var.broker_configs }) })) : jsonencode(l))
        ]
      })
    })
  })
}

# Kafka broker 用と Cruise Control 用は別 ConfigMap（名前・key が異なる）のため 1 つに統合しない。
# ファイル自体は公式 examples/metrics/ の配置に合わせ monitoring/ 側にあるが、Kafka CR が
# この ConfigMap に依存するため、apply の所有権は kafka module が持つ。
resource "kubernetes_manifest" "kafka_metrics" {
  manifest = yamldecode(file("${var.monitoring_manifest_dir}/kafka-metrics.yaml"))
}

resource "kubernetes_manifest" "cruise_control_metrics" {
  manifest = yamldecode(file("${var.monitoring_manifest_dir}/kafka-cruise-control-metrics.yaml"))
}

# broker/controller の I/O 特性は Prometheus/Alertmanager と異なるため、default の gp3 を共有せず
# 専用の StorageClass を用意する（IOPS/throughput を監視系と切り離して個別にチューニングできる）。
resource "kubernetes_manifest" "gp3_kafka_storage_class" {
  manifest = yamldecode(file("${var.kafka_manifest_dir}/gp3-kafka-storageclass.yaml"))
}

# KafkaNodePool の CRD は Strimzi Operator chart が導入する。chart 未導入のクラスタに apply すると
# CRD 未登録で失敗するため、呼び出し側で addons module への依存を張る。
resource "kubernetes_manifest" "controller_pool" {
  manifest = yamldecode(local.controller_pool_yaml)
}

resource "kubernetes_manifest" "broker_pool" {
  manifest = yamldecode(local.broker_pool_yaml)
}

resource "kubernetes_manifest" "kafka_cluster" {
  manifest = local.kafka_cluster

  depends_on = [
    kubernetes_manifest.kafka_metrics,
    kubernetes_manifest.cruise_control_metrics,
    kubernetes_manifest.controller_pool,
    kubernetes_manifest.broker_pool,
  ]
}

# AWS LBC が Service の EndpointSlice と TargetGroup を同期する。
# - Bootstrap: kafka-cluster-kafka-<listener>-bootstrap。bootstrap Service は Strimzi が pool 名と
#   無関係に固定文字列 kafka で生成するため、broker pool 名（broker）ではなく常に kafka を使う。
# - Broker N : kafka-cluster-broker-N。こちらは broker KafkaNodePool 名（broker）に依存する。
# この非対称は意図的であり統一しない。TargetGroup は ip 型のため、LBC は Pod IP を直接 target
# 登録し、ローリング更新による Pod の移動にも追従する。
# Service の port は number ではなく name（tcp-<listener>）で参照する。
resource "kubernetes_manifest" "target_group_binding" {
  for_each = var.target_group_arns

  manifest = yamldecode(templatestring(
    local.target_group_binding_template,
    {
      BINDING_NAME     = each.key == "Bootstrap" ? "kafka-${var.external_listener_name}-bootstrap" : "kafka-broker-${trimprefix(each.key, "Broker")}"
      SERVICE_NAME     = each.key == "Bootstrap" ? "kafka-cluster-kafka-${var.external_listener_name}-bootstrap" : "kafka-cluster-broker-${trimprefix(each.key, "Broker")}"
      SERVICE_PORT     = "tcp-${var.external_listener_name}"
      TARGET_GROUP_ARN = each.value
      NLB_SG_ID        = var.nlb_security_group_id
      TARGET_PORT      = tostring(var.target_port)
      VPC_ID           = var.vpc_id
    },
  ))

  depends_on = [kubernetes_manifest.kafka_cluster]
}

# Strimzi Topic Operator（entityOperator）が KafkaTopic CR を監視して実 Kafka に Topic を作成する。
# Operator は Kafka CR より後に起動するため、kafka_cluster への依存だけで apply 順序は十分。
resource "kubernetes_manifest" "test_topic" {
  manifest = yamldecode(templatestring(
    local.test_topic_template,
    { RETENTION_MS = var.topic_retention_ms },
  ))

  depends_on = [kubernetes_manifest.kafka_cluster]
}
