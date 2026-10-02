# 各 precondition は手前の欠落が原因で連鎖して失敗しないよう、手前が満たされているときだけ判定する。

output "external_listener_name" {
  description = "external listener の名前。Strimzi が生成する Service の port 名 tcp-<name> の組み立てに使う"
  value       = local.external.name

  precondition {
    condition     = local.listeners != null
    error_message = "kafka-cluster.yaml に spec.kafka.listeners が定義されているか確認してください。"
  }

  precondition {
    condition     = local.listeners == null || local.external != null
    error_message = "kafka-cluster.yaml に 'external' リスナーが見つかりません。"
  }
}

output "external_listener_port" {
  description = "external listener の port。全 broker の Service と NLB TargetGroup が共通で使う内部 port"
  value       = local.external_port

  precondition {
    condition     = local.external == null || local.external_port != null
    error_message = "kafka-cluster.yaml の external リスナーに 'port' が定義されていません。"
  }
}

output "nlb_ports" {
  description = "NLB の listener / TargetGroup 1 件ごとの { name, listener_port, node_port }。先頭が Bootstrap、続いて broker_count 件の Broker<N>"
  value       = local.nlb_ports

  precondition {
    condition     = local.external == null || local.configuration != null
    error_message = "kafka-cluster.yaml の external リスナーに configuration が定義されていません。"
  }

  precondition {
    condition     = local.configuration == null || local.bootstrap != null
    error_message = "kafka-cluster.yaml の external.configuration に 'bootstrap' が定義されていません。"
  }

  precondition {
    condition     = local.bootstrap == null || local.bootstrap_node_port != null
    error_message = "kafka-cluster.yaml の external.configuration.bootstrap に 'nodePort' が定義されていません。"
  }
}

output "broker_configs" {
  description = "Kafka CR の external listener の configuration.brokers[] に注入する設定"
  value       = local.broker_configs
}
