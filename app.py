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
# 本番環境では CompositePrincipal で運用者・CI/CD ロールの ARN を明示的に列挙すること
iam_stack = IamStack(
    app,
    "IamStack",
    admin_principal=iam.AccountRootPrincipal(),  # type: ignore[arg-type]
    env=env,
)

# Stack 1: VPC / EKS / アドオン / Kafka / 監視
# Kubernetes リソースは CDK が直接 apply する。
# ブローカー数変更時に EKS ノードグループ（AWS リソース）も連動するため
# GitOps のみでは完結せず、cdk deploy が常に必要。（詳細は README 参照）
infra_stack = EksCdkStack(
    app,
    "EksCdkStack",
    admin_role=cast(iam.IRole, iam_stack.eks_admin_role),
    env=env,
)
infra_stack.add_dependency(iam_stack)

app.synth()
