from aws_cdk import aws_eks_v2 as eks
from constructs import Construct


class KafkaConstruct(Construct):
    """
    StrimziオペレーターをArgoCD Applicationとしてデプロイし、
    KafkaクラスターCRを3ブローカー/3AZ構成でPrivateSubnetに配置する。
    外部接続はStrimziのloadbalancerリスナーを使用し、
    AWS Load Balancer ControllerがNLBを自動管理する。
    """

    def __init__(self, scope: Construct, construct_id: str, cluster: eks.Cluster) -> None:
        super().__init__(scope, construct_id)

        self._cluster = cluster

        strimzi_app = self._add_strimzi_operator()
        self._add_kafka_cluster(strimzi_app)

    def _add_strimzi_operator(self) -> eks.KubernetesManifest:
        return self._cluster.add_manifest(
            "StrimziOperatorApp",
            {
                "apiVersion": "argoproj.io/v1alpha1",
                "kind": "Application",
                "metadata": {
                    "name": "strimzi-operator",
                    "namespace": "argocd",
                    "annotations": {"argocd.argoproj.io/sync-wave": "0"},
                },
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

    def _add_kafka_cluster(self, strimzi_app: eks.KubernetesManifest) -> None:
        kafka_cr = self._cluster.add_manifest(
            "KafkaCluster",
            {
                "apiVersion": "kafka.strimzi.io/v1beta2",
                "kind": "Kafka",
                "metadata": {
                    "name": "kafka-cluster",
                    "namespace": "kafka",
                    "annotations": {"argocd.argoproj.io/sync-wave": "1"},
                },
                "spec": {
                    "kafka": {
                        "version": "3.9.0",
                        "replicas": 3,
                        "listeners": [
                            {"name": "plain", "port": 9092, "type": "internal", "tls": False},
                            {"name": "tls", "port": 9093, "type": "internal", "tls": True},
                            {
                                "name": "external",
                                "port": 9094,
                                "type": "loadbalancer",
                                "tls": True,
                                "configuration": {
                                    "bootstrap": {
                                        "annotations": {
                                            # AWS Load Balancer ControllerでNLBを作成
                                            "service.beta.kubernetes.io/aws-load-balancer-type": "external",
                                            "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "instance",
                                            # PrivateLinkはPrivate NLBが前提
                                            "service.beta.kubernetes.io/aws-load-balancer-scheme": "internal",
                                            # AZ障害耐性のためcross-zone load balancingを有効化
                                            "service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled": "true",
                                        }
                                    },
                                    "brokers": [
                                        {
                                            "broker": i,
                                            "annotations": {
                                                "service.beta.kubernetes.io/aws-load-balancer-type": "external",
                                                "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "instance",
                                                "service.beta.kubernetes.io/aws-load-balancer-scheme": "internal",
                                                "service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled": "true",
                                            },
                                        }
                                        for i in range(3)
                                    ],
                                },
                            },
                        ],
                        "config": {
                            "offsets.topic.replication.factor": 3,
                            "transaction.state.log.replication.factor": 3,
                            "transaction.state.log.min.isr": 2,
                            "default.replication.factor": 3,
                            "min.insync.replicas": 2,
                            "inter.broker.protocol.version": "3.9",
                        },
                        "storage": {
                            "type": "jbod",
                            "volumes": [
                                {
                                    "id": 0,
                                    "type": "persistent-claim",
                                    "size": "100Gi",
                                    "class": "gp3",
                                    "deleteClaim": False,
                                }
                            ],
                        },
                        "resources": {
                            "requests": {"memory": "4Gi", "cpu": "1000m"},
                            "limits": {"memory": "8Gi", "cpu": "2000m"},
                        },
                        "rack": {"topologyKey": "topology.kubernetes.io/zone"},
                        "template": {
                            "pod": {
                                "affinity": {
                                    "nodeAffinity": {
                                        "requiredDuringSchedulingIgnoredDuringExecution": {
                                            "nodeSelectorTerms": [
                                                {
                                                    "matchExpressions": [
                                                        {
                                                            "key": "role",
                                                            "operator": "In",
                                                            "values": ["kafka"],
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    },
                                    "podAntiAffinity": {
                                        "requiredDuringSchedulingIgnoredDuringExecution": [
                                            {
                                                "labelSelector": {
                                                    "matchExpressions": [
                                                        {
                                                            "key": "strimzi.io/name",
                                                            "operator": "In",
                                                            "values": ["kafka-cluster-kafka"],
                                                        }
                                                    ]
                                                },
                                                "topologyKey": "kubernetes.io/hostname",
                                            }
                                        ]
                                    },
                                }
                            }
                        },
                    },
                    "zookeeper": {
                        "replicas": 3,
                        "storage": {
                            "type": "persistent-claim",
                            "size": "10Gi",
                            "class": "gp3",
                            "deleteClaim": False,
                        },
                        "resources": {
                            "requests": {"memory": "1Gi", "cpu": "250m"},
                            "limits": {"memory": "2Gi", "cpu": "500m"},
                        },
                        "template": {
                            "pod": {
                                "affinity": {
                                    "podAntiAffinity": {
                                        "requiredDuringSchedulingIgnoredDuringExecution": [
                                            {
                                                "labelSelector": {
                                                    "matchExpressions": [
                                                        {
                                                            "key": "strimzi.io/name",
                                                            "operator": "In",
                                                            "values": ["kafka-cluster-zookeeper"],
                                                        }
                                                    ]
                                                },
                                                "topologyKey": "kubernetes.io/hostname",
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                    },
                    "entityOperator": {
                        "topicOperator": {},
                        "userOperator": {},
                    },
                },
            },
        )

        # Strimziオペレーターが先にデプロイされてからKafka CRを作成
        kafka_cr.node.add_dependency(strimzi_app)
