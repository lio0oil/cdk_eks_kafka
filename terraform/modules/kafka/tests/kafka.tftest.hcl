mock_provider "kubernetes" {}

variables {
  kafka_manifest_dir      = "../../../cdk/manifests/kafka"
  monitoring_manifest_dir = "../../../cdk/manifests/monitoring"
  broker_count            = 3
  controller_count        = 3
  delete_claim            = true
  broker_resources = {
    memory_request = "2Gi"
    memory_limit   = "4Gi"
    cpu_request    = "250m"
    cpu_limit      = "500m"
    storage_size   = "20Gi"
    jvm_xms        = "1024m"
    jvm_xmx        = "2048m"
  }
  controller_resources = {
    memory_request = "1Gi"
    memory_limit   = "2Gi"
    cpu_request    = "250m"
    cpu_limit      = "500m"
    storage_size   = "20Gi"
    jvm_xms        = "512m"
    jvm_xmx        = "1024m"
  }
  external_listener_name = "external"
  broker_configs = [
    { broker = 0, advertisedHost = "kafka.local", nodePort = 30095, advertisedPort = 9095 },
    { broker = 1, advertisedHost = "kafka.local", nodePort = 30096, advertisedPort = 9096 },
    { broker = 2, advertisedHost = "kafka.local", nodePort = 30097, advertisedPort = 9097 },
  ]
  target_group_arns = {
    Bootstrap = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-bootstrap/1"
    Broker0   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker0/2"
    Broker1   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker1/3"
    Broker2   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker2/4"
  }
  target_port           = 9094
  nlb_security_group_id = "sg-0123456789abcdef0"
  vpc_id                = "vpc-0123456789abcdef0"
  topic_retention_ms    = "604800000"
}

# Kafka broker 用と Cruise Control 用は Strimzi 公式サンプルでも別 ConfigMap（名前・key が異なる）
run "jmx_metrics_config_maps_are_separate" {
  command = apply

  assert {
    condition = (
      kubernetes_manifest.kafka_metrics.manifest.metadata.name == "kafka-metrics"
      && kubernetes_manifest.cruise_control_metrics.manifest.metadata.name == "cruise-control-metrics"
    )
    error_message = "Kafka / Cruise Control の JMX メトリクス ConfigMap が別々に適用されていない"
  }
}

run "kafka_has_its_own_storage_class" {
  command = apply

  assert {
    condition     = kubernetes_manifest.gp3_kafka_storage_class.manifest.metadata.name == "gp3-kafka"
    error_message = "broker/controller 専用の gp3-kafka StorageClass が適用されていない"
  }
}

run "controller_pool_gets_its_settings" {
  command = apply

  assert {
    condition = (
      kubernetes_manifest.controller_pool.manifest.metadata.name == "controller"
      && kubernetes_manifest.controller_pool.manifest.spec.replicas == 3
      && kubernetes_manifest.controller_pool.manifest.spec.storage.volumes[0].deleteClaim == true
      && kubernetes_manifest.controller_pool.manifest.spec.storage.volumes[0].size == "20Gi"
    )
    error_message = "controller pool の replicas / deleteClaim / storage が期待と異なる"
  }

  assert {
    condition = (
      kubernetes_manifest.controller_pool.manifest.spec.resources.requests.memory == "1Gi"
      && kubernetes_manifest.controller_pool.manifest.spec.resources.limits.memory == "2Gi"
      && kubernetes_manifest.controller_pool.manifest.spec.resources.requests.cpu == "250m"
      && kubernetes_manifest.controller_pool.manifest.spec.jvmOptions["-Xmx"] == "1024m"
    )
    error_message = "controller pool の resources / jvmOptions が期待と異なる"
  }
}

run "broker_pool_follows_broker_count_and_settings" {
  command = apply

  variables {
    broker_count = 5
    delete_claim = false
    target_group_arns = {
      Bootstrap = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-bootstrap/1"
      Broker0   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker0/2"
      Broker1   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker1/3"
      Broker2   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker2/4"
      Broker3   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker3/5"
      Broker4   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker4/6"
    }
  }

  assert {
    condition = (
      kubernetes_manifest.broker_pool.manifest.metadata.name == "broker"
      && kubernetes_manifest.broker_pool.manifest.spec.replicas == 5
      && kubernetes_manifest.broker_pool.manifest.spec.storage.volumes[0].deleteClaim == false
    )
    error_message = "broker_count / delete_claim が broker pool に反映されていない"
  }

  assert {
    condition = (
      kubernetes_manifest.broker_pool.manifest.spec.resources.requests.memory == "2Gi"
      && kubernetes_manifest.broker_pool.manifest.spec.resources.limits.cpu == "500m"
      && kubernetes_manifest.broker_pool.manifest.spec.jvmOptions["-Xms"] == "1024m"
    )
    error_message = "broker pool の resources / jvmOptions が期待と異なる"
  }
}

# broker_count を単一の真実の源にするため、brokers[] は YAML に書かずここで注入する
run "external_listener_gets_the_generated_brokers" {
  command = apply

  assert {
    condition = jsonencode([
      for l in kubernetes_manifest.kafka_cluster.manifest.spec.kafka.listeners : l.configuration.brokers if l.name == "external"
    ][0]) == jsonencode(var.broker_configs)
    error_message = "external listener の configuration.brokers が broker_configs と一致しない"
  }

  # 注入は external listener だけに行い、他の listener や bootstrap の設定は壊さない
  assert {
    condition = (
      [for l in kubernetes_manifest.kafka_cluster.manifest.spec.kafka.listeners : l.port if l.name == "plain"][0] == 9092
      && [for l in kubernetes_manifest.kafka_cluster.manifest.spec.kafka.listeners : l.configuration.bootstrap.nodePort if l.name == "external"][0] == 30094
      && kubernetes_manifest.kafka_cluster.manifest.metadata.name == "kafka-cluster"
    )
    error_message = "external 以外の listener や bootstrap の設定が保持されていない"
  }
}

run "target_group_bindings_use_the_right_service_names" {
  command = apply

  # bootstrap Service は Strimzi が pool 名と無関係に固定文字列 kafka で生成する
  # （kafka-cluster-kafka-<listener>-bootstrap）。broker 個別 Service だけが pool 名（broker）に依存する。
  assert {
    condition = (
      kubernetes_manifest.target_group_binding["Bootstrap"].manifest.metadata.name == "kafka-external-bootstrap"
      && kubernetes_manifest.target_group_binding["Bootstrap"].manifest.spec.serviceRef.name == "kafka-cluster-kafka-external-bootstrap"
      && kubernetes_manifest.target_group_binding["Broker1"].manifest.metadata.name == "kafka-broker-1"
      && kubernetes_manifest.target_group_binding["Broker1"].manifest.spec.serviceRef.name == "kafka-cluster-broker-1"
    )
    error_message = "TargetGroupBinding の名前 / serviceRef.name が bootstrap と broker で期待どおりに非対称になっていない"
  }

  assert {
    condition = alltrue([
      for k, b in kubernetes_manifest.target_group_binding :
      b.manifest.spec.serviceRef.port == "tcp-external"
      && b.manifest.spec.targetGroupARN == var.target_group_arns[k]
      && b.manifest.spec.targetType == "ip"
      && b.manifest.spec.vpcID == "vpc-0123456789abcdef0"
      && b.manifest.spec.networking.ingress[0].from[0].securityGroup.groupID == "sg-0123456789abcdef0"
      && b.manifest.spec.networking.ingress[0].ports[0].port == 9094
    ])
    error_message = "TargetGroupBinding の port 名 / TargetGroup ARN / targetType / VPC / SG / port が期待と異なる"
  }
}

run "target_group_bindings_follow_target_groups" {
  command = apply

  variables {
    broker_count = 4
    target_group_arns = {
      Bootstrap = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-bootstrap/1"
      Broker0   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker0/2"
      Broker1   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker1/3"
      Broker2   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker2/4"
      Broker3   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker3/5"
    }
  }

  assert {
    condition     = length(kubernetes_manifest.target_group_binding) == 5
    error_message = "broker 追加時に TargetGroupBinding が連動して増えない"
  }
}

run "test_topic_gets_the_retention" {
  command = apply

  assert {
    condition = (
      kubernetes_manifest.test_topic.manifest.metadata.name == "test-topic"
      && tostring(kubernetes_manifest.test_topic.manifest.spec.config["retention.ms"]) == "604800000"
    )
    error_message = "test-topic の retention.ms が反映されていない"
  }
}

# TargetGroup は platform 側が broker_count から作るため、broker_count と食い違うと
# 一部の broker が NLB から到達できない（または存在しない broker を指す）。
run "target_groups_must_match_broker_count" {
  command = plan

  variables {
    broker_count = 5
  }

  expect_failures = [var.target_group_arns]
}
