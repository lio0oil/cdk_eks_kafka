from aws_cdk import Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct


class AddonsConstruct(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, cluster: eks.ICluster
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster

        self._add_eks_addons()
        self._add_argocd()
        self._add_ack_controllers()

    def _add_eks_addons(self) -> None:
        # EBS CSI Driver用 IRSA
        ebs_csi_sa = self._cluster.add_service_account(
            "EbsCsiSa",
            name="ebs-csi-controller-sa",
            namespace="kube-system",
        )
        ebs_csi_sa.node.add_dependency(self._cluster)
        ebs_csi_sa.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonEBSCSIDriverPolicy"
            )
        )

        for addon_name, construct_id in {
            "vpc-cni": "VpcCni",
            "coredns": "CoreDns",
            "kube-proxy": "KubeProxy",
            "eks-pod-identity-agent": "PodIdentityAgent",
        }.items():
            eks.Addon(self, construct_id, cluster=self._cluster, addon_name=addon_name)

        eks.Addon(
            self,
            "EbsCsiDriver",
            cluster=self._cluster,
            addon_name="aws-ebs-csi-driver",
            configuration_values={
                "serviceAccount": {
                    "annotations": {
                        "eks.amazonaws.com/role-arn": ebs_csi_sa.role.role_arn
                    }
                }
            },
        )

    def _add_ack_controllers(self) -> None:
        region = Stack.of(self).region

        # 1. ACK EC2 Controller (VPC Endpoint Service 管理用)
        ack_ec2_sa = self._cluster.add_service_account(
            "AckEc2Sa",
            name="ack-ec2-controller",
            namespace="ack-system",
        )
        ack_ec2_sa.role.attach_inline_policy(
            iam.Policy(
                self,
                "AckEc2Policy",
                statements=[
                    iam.PolicyStatement(
                        actions=[
                            "ec2:CreateVpcEndpointServiceConfiguration",
                            "ec2:DeleteVpcEndpointServiceConfigurations",
                            "ec2:DescribeVpcEndpointServiceConfigurations",
                            "ec2:ModifyVpcEndpointServiceConfiguration",
                            "ec2:ModifyVpcEndpointServicePermissions",
                            "ec2:DescribeVpcEndpointServicePermissions",
                            "ec2:DescribeVpcEndpointConnections",
                            "ec2:AcceptVpcEndpointConnections",
                            "ec2:RejectVpcEndpointConnections",
                        ],
                        resources=["*"],
                    )
                ],
            )
        )

        ec2_helm = self._cluster.add_helm_chart(
            "AckEc2Controller",
            chart="ec2-chart",
            repository="oci://public.ecr.aws/aws-controllers-k8s/ec2-chart",
            namespace="ack-system",
            create_namespace=True,
            version="v1.2.14",
            values={
                "aws": {"region": region},
                "serviceAccount": {"create": False, "name": "ack-ec2-controller"},
            },
        )
        ec2_helm.node.add_dependency(ack_ec2_sa)

        # 2. ACK ELBv2 Controller (Listener / TargetGroup 管理用)
        ack_elbv2_sa = self._cluster.add_service_account(
            "AckElbv2Sa",
            name="ack-elbv2-controller",
            namespace="ack-system",
        )
        ack_elbv2_sa.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "ElasticLoadBalancingFullAccess"
            )
        )

        elbv2_helm = self._cluster.add_helm_chart(
            "AckElbv2Controller",
            chart="elbv2-chart",
            repository="oci://public.ecr.aws/aws-controllers-k8s/elbv2-chart",
            namespace="ack-system",
            version="v1.1.8",
            values={
                "aws": {"region": region},
                "serviceAccount": {"create": False, "name": "ack-elbv2-controller"},
            },
        )
        elbv2_helm.node.add_dependency(ack_elbv2_sa)

    def _add_argocd(self) -> None:
        self._cluster.add_manifest(
            "Gp3StorageClass",
            {
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "gp3",
                    "annotations": {
                        "storageclass.kubernetes.io/is-default-class": "true"
                    },
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

        repo_url: str = self.node.get_context("repo-url")

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
                    "tolerations": [
                        {"key": "CriticalAddonsOnly", "operator": "Exists"}
                    ],
                    "nodeSelector": {"role": "system"},
                },
                "applicationSet": {"replicaCount": 2},
                "controller": {
                    "tolerations": [
                        {"key": "CriticalAddonsOnly", "operator": "Exists"}
                    ],
                    "nodeSelector": {"role": "system"},
                },
                "redis": {
                    "tolerations": [
                        {"key": "CriticalAddonsOnly", "operator": "Exists"}
                    ],
                    "nodeSelector": {"role": "system"},
                },
                "repoServer": {
                    "tolerations": [
                        {"key": "CriticalAddonsOnly", "operator": "Exists"}
                    ],
                    "nodeSelector": {"role": "system"},
                },
                "configs": {
                    "repositories": {
                        "github": {
                            "type": "git",
                            "url": repo_url,
                        }
                    }
                },
            },
        )

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
