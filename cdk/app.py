#!/usr/bin/env python3
import os
from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_iam as iam

from ekscdk.config import ClusterConfig
from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.iam_stack import IamStack
from ekscdk.test_ec2_stack import TestEc2Stack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

# -c env=dev|stg|prd でデプロイ環境を選択する（省略時は dev）
_env_name: str = app.node.try_get_context("env") or "dev"
_config_factories = {
    "dev": ClusterConfig.for_dev,
    "stg": ClusterConfig.for_stg,
    "prd": ClusterConfig.for_prd,
}
if _env_name not in _config_factories:
    raise ValueError(f"不明な env: {_env_name!r}。dev / stg / prd のいずれかを指定してください。")
config = _config_factories[_env_name]()

# Stack 0: IAM ロール（EksCdkStack より先にデプロイ）
# 本番環境では CompositePrincipal で運用者・CI/CD ロールの ARN を明示的に列挙すること
iam_stack = IamStack(
    app,
    "IamStack",
    admin_principal=iam.AccountRootPrincipal(),  # type: ignore[arg-type]
    role_name=config.admin_role_name,
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
    config=config,
    env=env,
)
infra_stack.add_dependency(iam_stack)

# Stack 2: テスト用 EC2（dev のみ）
# Session Manager で接続して EKS / Kafka NLB を検証する踏み台。
# stg/prd には残さない（恒久踏み台が必要になった時点で別途検討）。
if _env_name == "dev":
    test_ec2_stack = TestEc2Stack(
        app,
        "TestEc2Stack",
        vpc=infra_stack.vpc,
        env=env,
    )
    test_ec2_stack.add_dependency(infra_stack)

app.synth()
