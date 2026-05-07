from typing import cast

from aws_cdk import Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct


class AddonsConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, cluster: eks.ICluster) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster

        self._add_eks_addons()
        self._add_aws_load_balancer_controller()
        self._add_argocd()

    def _add_eks_addons(self) -> None:
        # EBS CSI Driver用 IRSA
        ebs_csi_sa = self._cluster.add_service_account(
            "EbsCsiSa",
            name="ebs-csi-controller-sa",
            namespace="kube-system",
        )
        ebs_csi_sa.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEBSCSIDriverPolicy")
        )

        # L2 Addon コンストラクトを使用
        eks.Addon(
            self,
            "VpcCni",
            cluster=self._cluster,
            addon_name="vpc-cni",
        )

        eks.Addon(
            self,
            "CoreDns",
            cluster=self._cluster,
            addon_name="coredns",
        )

        eks.Addon(
            self,
            "KubeProxy",
            cluster=self._cluster,
            addon_name="kube-proxy",
        )

        eks.Addon(
            self,
            "PodIdentityAgent",
            cluster=self._cluster,
            addon_name="eks-pod-identity-agent",
        )

        eks.Addon(
            self,
            "EbsCsiDriver",
            cluster=self._cluster,
            addon_name="aws-ebs-csi-driver",
            configuration_values={"serviceAccount": {"annotations": {"eks.amazonaws.com/role-arn": ebs_csi_sa.role.role_arn}}},
        )

    def _add_aws_load_balancer_controller(self) -> None:
        lbc_sa = self._cluster.add_service_account(
            "AwsLbcSa",
            name="aws-load-balancer-controller",
            namespace="kube-system",
        )

        for stmt in [
            iam.PolicyStatement(
                actions=["iam:CreateServiceLinkedRole"],
                resources=["*"],
                conditions={"StringEquals": {"iam:AWSServiceName": "elasticloadbalancing.amazonaws.com"}},
            ),
            iam.PolicyStatement(
                actions=[
                    "ec2:DescribeAccountAttributes",
                    "ec2:DescribeAddresses",
                    "ec2:DescribeAvailabilityZones",
                    "ec2:DescribeInternetGateways",
                    "ec2:DescribeVpcs",
                    "ec2:DescribeVpcPeeringConnections",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeInstances",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeTags",
                    "ec2:GetCoipPoolUsage",
                    "ec2:DescribeCoipPools",
                    "elasticloadbalancing:DescribeLoadBalancers",
                    "elasticloadbalancing:DescribeLoadBalancerAttributes",
                    "elasticloadbalancing:DescribeListeners",
                    "elasticloadbalancing:DescribeListenerCertificates",
                    "elasticloadbalancing:DescribeSSLPolicies",
                    "elasticloadbalancing:DescribeRules",
                    "elasticloadbalancing:DescribeTargetGroups",
                    "elasticloadbalancing:DescribeTargetGroupAttributes",
                    "elasticloadbalancing:DescribeTargetHealth",
                    "elasticloadbalancing:DescribeTags",
                    "elasticloadbalancing:DescribeTrustStores",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=[
                    "cognito-idp:DescribeUserPoolClient",
                    "acm:ListCertificates",
                    "acm:DescribeCertificate",
                    "iam:ListServerCertificates",
                    "iam:GetServerCertificate",
                    "waf-regional:GetWebACL",
                    "waf-regional:GetWebACLForResource",
                    "waf-regional:AssociateWebACL",
                    "waf-regional:DisassociateWebACL",
                    "wafv2:GetWebACL",
                    "wafv2:GetWebACLForResource",
                    "wafv2:AssociateWebACL",
                    "wafv2:DisassociateWebACL",
                    "shield:GetSubscriptionState",
                    "shield:DescribeProtection",
                    "shield:CreateProtection",
                    "shield:DeleteProtection",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress", "ec2:CreateSecurityGroup"],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=["ec2:CreateTags"],
                resources=["arn:aws:ec2:*:*:security-group/*"],
                conditions={
                    "StringEquals": {"ec2:CreateAction": "CreateSecurityGroup"},
                    "Null": {"aws:RequestTag/elbv2.k8s.aws/cluster": "false"},
                },
            ),
            iam.PolicyStatement(
                actions=["ec2:CreateTags", "ec2:DeleteTags"],
                resources=["arn:aws:ec2:*:*:security-group/*"],
                conditions={
                    "Null": {
                        "aws:RequestTag/elbv2.k8s.aws/cluster": "true",
                        "aws:ResourceTag/elbv2.k8s.aws/cluster": "false",
                    }
                },
            ),
            iam.PolicyStatement(
                actions=["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress", "ec2:DeleteSecurityGroup"],
                resources=["*"],
                conditions={"Null": {"aws:ResourceTag/elbv2.k8s.aws/cluster": "false"}},
            ),
            iam.PolicyStatement(
                actions=["elasticloadbalancing:CreateLoadBalancer", "elasticloadbalancing:CreateTargetGroup"],
                resources=["*"],
                conditions={"Null": {"aws:RequestTag/elbv2.k8s.aws/cluster": "false"}},
            ),
            iam.PolicyStatement(
                actions=[
                    "elasticloadbalancing:CreateListener",
                    "elasticloadbalancing:DeleteListener",
                    "elasticloadbalancing:CreateRule",
                    "elasticloadbalancing:DeleteRule",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=["elasticloadbalancing:AddTags", "elasticloadbalancing:RemoveTags"],
                resources=[
                    "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*",
                    "arn:aws:elasticloadbalancing:*:*:loadbalancer/net/*/*",
                    "arn:aws:elasticloadbalancing:*:*:loadbalancer/app/*/*",
                ],
                conditions={
                    "Null": {
                        "aws:RequestTag/elbv2.k8s.aws/cluster": "true",
                        "aws:ResourceTag/elbv2.k8s.aws/cluster": "false",
                    }
                },
            ),
            iam.PolicyStatement(
                actions=["elasticloadbalancing:AddTags", "elasticloadbalancing:RemoveTags"],
                resources=[
                    "arn:aws:elasticloadbalancing:*:*:listener/net/*/*/*",
                    "arn:aws:elasticloadbalancing:*:*:listener/app/*/*/*",
                    "arn:aws:elasticloadbalancing:*:*:listener-rule/net/*/*/*",
                    "arn:aws:elasticloadbalancing:*:*:listener-rule/app/*/*/*",
                ],
            ),
            iam.PolicyStatement(
                actions=[
                    "elasticloadbalancing:ModifyLoadBalancerAttributes",
                    "elasticloadbalancing:SetIpAddressType",
                    "elasticloadbalancing:SetSecurityGroups",
                    "elasticloadbalancing:SetSubnets",
                    "elasticloadbalancing:DeleteLoadBalancer",
                    "elasticloadbalancing:ModifyTargetGroup",
                    "elasticloadbalancing:ModifyTargetGroupAttributes",
                    "elasticloadbalancing:DeleteTargetGroup",
                ],
                resources=["*"],
                conditions={"Null": {"aws:ResourceTag/elbv2.k8s.aws/cluster": "false"}},
            ),
            iam.PolicyStatement(
                actions=["elasticloadbalancing:AddTags"],
                resources=[
                    "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*",
                    "arn:aws:elasticloadbalancing:*:*:loadbalancer/net/*/*",
                    "arn:aws:elasticloadbalancing:*:*:loadbalancer/app/*/*",
                ],
                conditions={
                    "StringEquals": {"elasticloadbalancing:CreateAction": ["CreateTargetGroup", "CreateLoadBalancer"]},
                    "Null": {"aws:RequestTag/elbv2.k8s.aws/cluster": "false"},
                },
            ),
            iam.PolicyStatement(
                actions=["elasticloadbalancing:RegisterTargets", "elasticloadbalancing:DeregisterTargets"],
                resources=["arn:aws:elasticloadbalancing:*:*:targetgroup/*/*"],
            ),
            iam.PolicyStatement(
                actions=[
                    "elasticloadbalancing:SetWebAcl",
                    "elasticloadbalancing:ModifyListener",
                    "elasticloadbalancing:AddListenerCertificates",
                    "elasticloadbalancing:RemoveListenerCertificates",
                    "elasticloadbalancing:ModifyRule",
                ],
                resources=["*"],
            ),
        ]:
            cast(iam.Role, lbc_sa.role).add_to_policy(stmt)

        self._cluster.add_helm_chart(
            "AwsLoadBalancerController",
            chart="aws-load-balancer-controller",
            repository="https://aws.github.io/eks-charts",
            namespace="kube-system",
            version="1.11.0",
            values={
                "clusterName": self._cluster.cluster_name,
                "serviceAccount": {"create": False, "name": "aws-load-balancer-controller"},
                "vpcId": self._cluster.vpc.vpc_id,
                "region": Stack.of(self).region,
                "replicaCount": 2,
                "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                "nodeSelector": {"role": "system"},
            },
        )

    def _add_argocd(self) -> None:
        # gp3 をデフォルトStorageClassとして設定（EBS CSI Driver依存）
        self._cluster.add_manifest(
            "Gp3StorageClass",
            {
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "gp3",
                    "annotations": {"storageclass.kubernetes.io/is-default-class": "true"},
                },
                "provisioner": "ebs.csi.aws.com",
                "volumeBindingMode": "WaitForFirstConsumer",
                "reclaimPolicy": "Retain",
                "parameters": {
                    "type": "gp3",
                    "encrypted": "true",
                },
            },
        )

        repo_url: str = self.node.get_context("repo-url")  # 例: https://github.com/<org>/<repo>

        argocd = self._cluster.add_helm_chart(
            "ArgoCD",
            chart="argo-cd",
            repository="https://argoproj.github.io/argo-helm",
            namespace="argocd",
            create_namespace=True,
            version="7.8.23",
            values={
                "server": {
                    "replicas": 2,
                    "autoscaling": {"enabled": True, "minReplicas": 2},
                    "service": {"type": "ClusterIP"},
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "applicationSet": {"replicaCount": 2},
                "controller": {
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "redis": {
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "repoServer": {
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "configs": {
                    # GitHub リポジトリを ArgoCD に登録
                    # プライベートリポジトリの場合は deploy 後に
                    # `argocd repo add <repo-url> --username x-token --password <PAT>` で認証設定
                    "repositories": {
                        "github": {
                            "type": "git",
                            "url": repo_url,
                        }
                    }
                },
            },
        )

        # ArgoCD Application はすべて CDK で管理する
        # （ArgoCDのApplicationはインフラ設定。KafkaのCRのみGitで変更管理）

        # Strimzi オペレーター
        strimzi = self._cluster.add_manifest(
            "StrimziOperatorApp",
            {
                "apiVersion": "argoproj.io/v1alpha1",
                "kind": "Application",
                "metadata": {"name": "strimzi-operator", "namespace": "argocd"},
                "spec": {
                    "project": "default",
                    "source": {
                        "repoURL": "https://strimzi.io/charts/",
                        "chart": "strimzi-kafka-operator",
                        "targetRevision": "0.45.0",
                        "helm": {
                            "valuesObject": {
                                "watchNamespaces": ["kafka"],
                                "replicas": 1,
                            }
                        },
                    },
                    "destination": {
                        "server": "https://kubernetes.default.svc",
                        "namespace": "strimzi-operator",
                    },
                    "syncPolicy": {
                        "automated": {"prune": True, "selfHeal": True},
                        "syncOptions": ["CreateNamespace=true"],
                    },
                },
            },
        )
        strimzi.node.add_dependency(argocd)

        # Kafka Cluster Application: manifests/kafka/ を監視
        # git push するだけで Kafka 設定がクラスターに反映される
        kafka_app = self._cluster.add_manifest(
            "KafkaClusterApp",
            {
                "apiVersion": "argoproj.io/v1alpha1",
                "kind": "Application",
                "metadata": {"name": "kafka-cluster", "namespace": "argocd"},
                "spec": {
                    "project": "default",
                    "source": {
                        "repoURL": repo_url,
                        "targetRevision": "HEAD",
                        "path": "manifests/kafka",
                    },
                    "destination": {
                        "server": "https://kubernetes.default.svc",
                        "namespace": "kafka",
                    },
                    "syncPolicy": {
                        "automated": {"prune": True, "selfHeal": True},
                        "syncOptions": ["CreateNamespace=true"],
                    },
                },
            },
        )
        kafka_app.node.add_dependency(strimzi)
