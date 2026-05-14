from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from constructs import Construct


class IamStack(Stack):
    """EKS 管理者ロールスタック

    EksCdkStack より先にデプロイする。

    ## 複数のプリンシパルに admin 権限を付与したい場合

    複数の運用者・CI/CD ロールに cluster-admin を付与するには、
    admin_principal に CompositePrincipal で列挙して渡す。
    各プリンシパルは `aws eks get-token` で eks-cluster-admin を assume してから kubectl を使う。

    例:
        admin_principal=iam.CompositePrincipal(
            iam.ArnPrincipal("arn:aws:iam::<account>:role/SsoOpsRole"),
            iam.ArnPrincipal("arn:aws:iam::<account>:role/GitHubActionsRole"),
        )

    cluster-admin より権限を絞ったロール（read-only 等）が必要な場合は
    Kubernetes の ClusterRoleBinding で別途付与する（IAM ロールを追加し
    EKS Access Entry に登録）。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        admin_principal: iam.IPrincipal,
        role_name: str = "eks-cluster-admin",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.eks_admin_role = iam.Role(
            self,
            "EksAdminRole",
            role_name=role_name,
            description="EKS cluster-admin role (Kubernetes system:masters).",
            assumed_by=admin_principal,
        )
