mock_provider "aws" {
  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-bucket"
    }
  }
}

variables {
  cluster_name                = "eks-cluster-dev"
  vpc_id                      = "vpc-0123456789abcdef0"
  kafka_nlb_security_group_id = "sg-0123456789abcdef0"
  force_destroy               = true
}

# SG を作ると既定の全許可 egress が外れるため、必要な送信先だけを明示する
run "esm_poller_can_only_reach_the_kafka_nlb_and_aws_apis" {
  command = apply

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.to_kafka_nlb.referenced_security_group_id == "sg-0123456789abcdef0"
      && aws_vpc_security_group_egress_rule.to_kafka_nlb.ip_protocol == "tcp"
      && aws_vpc_security_group_egress_rule.to_kafka_nlb.from_port == 0
      && aws_vpc_security_group_egress_rule.to_kafka_nlb.to_port == 65535
    )
    error_message = "NLB の SG 宛ての全 TCP port 送信だけが許可されていない（broker ごとに port が異なるため単一 port には絞れない）"
  }

  # Lambda invoke API と STS への到達性は ESM の VPC アクセス要件。これらには宛先を絞れる
  # VPC エンドポイント用のプレフィックスリストが無いため 0.0.0.0/0:443 とする
  assert {
    condition = (
      aws_vpc_security_group_egress_rule.https_to_aws_apis.cidr_ipv4 == "0.0.0.0/0"
      && aws_vpc_security_group_egress_rule.https_to_aws_apis.ip_protocol == "tcp"
      && aws_vpc_security_group_egress_rule.https_to_aws_apis.from_port == 443
      && aws_vpc_security_group_egress_rule.https_to_aws_apis.to_port == 443
    )
    error_message = "AWS API 向けの HTTPS（443）送信が許可されていない"
  }

  assert {
    condition     = aws_security_group.esm.vpc_id == "vpc-0123456789abcdef0" && length(aws_security_group.esm.egress) == 0
    error_message = "ESM の SG が既定の全許可 egress を持っている、または VPC が異なる"
  }
}

run "outputs_expose_the_esm_security_group_and_bucket_arns" {
  command = apply

  assert {
    condition     = output.esm_security_group_id == aws_security_group.esm.id
    error_message = "esm_security_group_id が ESM の SG を指していない"
  }

  assert {
    condition     = output.data_bucket_arn == module.data_bucket.arn && output.artifact_bucket_arn == module.artifact_bucket.arn
    error_message = "バケット ARN の出力が各バケットを指していない"
  }
}
