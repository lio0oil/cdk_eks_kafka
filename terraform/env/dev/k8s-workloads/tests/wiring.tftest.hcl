# infra の出力（remote state）と tfvars が各 module に渡ることを mock で検証する。
# command = apply は mock provider に対する適用で、実クラスターには触れない。

mock_provider "kubernetes" {}

override_data {
  target = data.terraform_remote_state.infra
  values = {
    outputs = {
      cluster_name                       = "eks-cluster-dev"
      cluster_endpoint                   = "https://0123456789ABCDEF.gr7.ap-northeast-1.eks.amazonaws.com"
      cluster_certificate_authority_data = "bW9jaw=="
      vpc_id                             = "vpc-0123456789abcdef0"
      kafka_private_dns_name             = "kafka.local"
      broker_count                       = 3
      kafka_controller_count             = 3
      kafka_nlb_security_group_id        = "sg-0123456789abcdef0"
      kafka_target_group_arns = {
        Bootstrap = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-bootstrap/1"
        Broker0   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker0/2"
        Broker1   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker1/3"
        Broker2   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker2/4"
      }
    }
  }
}

# broker 数は infra の出力が単一の真実の源。Bootstrap + broker 3 件の port が導出され、
# Kafka / 監視の全リソースが apply できる。
run "infra_outputs_flow_into_kafka_and_monitoring_resources" {
  command = apply

  assert {
    condition     = length(module.kafka_config.nlb_ports) == 4 && module.kafka_config.external_listener_port == 9094
    error_message = "infra の broker_count から kafka-cluster.yaml 由来の port が導出されていない"
  }
}
