#!/usr/bin/env python3
import os

import aws_cdk as cdk

from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.kafka_stack import KafkaStack
from ekscdk.privatelink_stack import PrivateLinkStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Stack 1: VPC / EKS / アドオン / ArgoCD
infra_stack = EksCdkStack(app, "EksCdkStack", env=env)

# Stack 2: Strimzi + Kafka CR（ArgoCD経由でNLBも自動作成）
kafka_stack = KafkaStack(app, "KafkaStack", cluster=infra_stack.cluster, env=env)
kafka_stack.add_dependency(infra_stack)

# Stack 3: PrivateLink（KafkaのNLB作成後にデプロイ）
# cdk deploy PrivateLinkStack -c kafka-bootstrap-nlb-arn=<ARN>
privatelink_stack = PrivateLinkStack(app, "PrivateLinkStack", env=env)
privatelink_stack.add_dependency(kafka_stack)

app.synth()
