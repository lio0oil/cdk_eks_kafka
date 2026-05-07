# EKS on EC2 CDK Sample
- 言語はPython
- Stackは1つ。Constructの継承を利用する。
- EKSのバージョン、KubectlLayerのバージョン等すべて最新を利用する。
- EKSの作成はaws_eks_v2を利用する。
- ManagedNodeGroups、Node Auto Repair
- インフラとArgo CDまではCDK
- StrimziでKafkaを構築
- AZは3
- Kafkaはprivatesubnet