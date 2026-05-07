#!/usr/bin/env python3
import os

import aws_cdk as cdk

from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.privatelink_stack import PrivateLinkStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Stack 1: VPC / EKS / アドオン / ArgoCD + Bootstrap Application
# 必須: -c repo-url=<GitリポジトリURL>
# git push → ArgoCD が manifests/ を自動同期
infra_stack = EksCdkStack(app, "EksCdkStack", env=env)

# Stack 2: PrivateLink（KafkaのNLB作成後にデプロイ）
# 必須: -c kafka-bootstrap-nlb-arn=<NLB ARN>
privatelink_stack = PrivateLinkStack(app, "PrivateLinkStack", env=env)
privatelink_stack.add_dependency(infra_stack)

app.synth()
