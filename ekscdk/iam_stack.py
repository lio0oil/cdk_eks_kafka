from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from constructs import Construct


class IamStack(Stack):
    """EKS 管理者ロールスタック

    EksCdkStack より先にデプロイする。
    デフォルトの trust policy はアカウントルート（開発用）。
    本番では assumed_by を SSO Permission Set や CI/CD ロールの ARN に変更する。
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.eks_admin_role = iam.Role(
            self,
            "EksAdminRole",
            role_name="eks-cluster-admin",
            description="EKS cluster-admin role (Kubernetes system:masters). Restrict assumed_by for production.",
            assumed_by=iam.AccountRootPrincipal(),  # type: ignore[arg-type]
        )
