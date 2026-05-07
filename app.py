#!/usr/bin/env python3
import os
from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_iam as iam

from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.iam_stack import IamStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# Stack 0: IAM ロール（EksCdkStack より先にデプロイ）
iam_stack = IamStack(app, "IamStack", env=env)

# Stack 1: VPC / EKS / アドオン / ArgoCD + Bootstrap Application
# 必須: -c repo-url=<GitリポジトリURL>
# git push → ArgoCD が manifests/ を自動同期
infra_stack = EksCdkStack(
    app,
    "EksCdkStack",
    admin_role=cast(iam.IRole, iam_stack.eks_admin_role),
    env=env,
)
infra_stack.add_dependency(iam_stack)

app.synth()
