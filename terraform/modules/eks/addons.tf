# vpc-cni / kube-proxy はノードが kubelet 登録後に Ready になるための前提コンポーネント。
# nodegroup 作成より前に導入する（node_groups.tf の depends_on 参照）。
resource "aws_eks_addon" "before_compute" {
  for_each = toset(["vpc-cni", "kube-proxy"])

  cluster_name  = aws_eks_cluster.this.name
  addon_name    = each.key
  addon_version = var.addon_versions[each.key]
}

# ここから下は Pod をスケジュールするノードを要する Addon（Deployment / DaemonSet を持つ）。
# ノードが無い状態で作ると ACTIVE に到達できず作成がタイムアウトするため、
# いずれも全 nodegroup の作成完了を待たせる。
locals {
  # eks-pod-identity-agent は DaemonSet のためノード 0 台でも ACTIVE になってしまう。nodegroup を
  # 待たせて ACTIVE 到達が実際の Pod 起動を伴うようにする（Pod Identity でクレデンシャルを得る
  # ワークロードの前提）。バージョンは EKS の既定に追従させる。
  after_compute_addons = {
    "coredns"                = { version = var.addon_versions["coredns"], configuration_values = null }
    "eks-pod-identity-agent" = { version = null, configuration_values = null }
    "aws-ebs-csi-driver"     = { version = var.addon_versions["aws-ebs-csi-driver"], configuration_values = null }
    "metrics-server"         = { version = var.addon_versions["metrics-server"], configuration_values = null }
    "snapshot-controller"    = { version = var.addon_versions["snapshot-controller"], configuration_values = null }
    "eks-node-monitoring-agent" = {
      version = var.addon_versions["eks-node-monitoring-agent"]
      # --verbosity は zap level に -1 倍して渡されるため、WARN 以上にするには -1。
      # additionalArgs は完全置換なので chart 既定の --metrics-address も含める。
      configuration_values = jsonencode({
        nodeAgent = { additionalArgs = ["--metrics-address=:8003", "--verbosity=-1"] }
      })
    }
  }
}

resource "aws_eks_addon" "after_compute" {
  for_each = local.after_compute_addons

  cluster_name         = aws_eks_cluster.this.name
  addon_name           = each.key
  addon_version        = each.value.version
  configuration_values = each.value.configuration_values

  # aws-ebs-csi-driver は ebs-csi-controller-sa という ServiceAccount を Addon 自身が作るため、
  # こちらでは作らない（事前に同名 SA があると衝突する）。ただし Pod Identity の Role は
  # ワークロード固有の権限設計であり Addon 側が自動生成できないため、Role は下で用意する。
  dynamic "pod_identity_association" {
    for_each = each.key == "aws-ebs-csi-driver" ? [1] : []

    content {
      role_arn        = aws_iam_role.ebs_csi.arn
      service_account = "ebs-csi-controller-sa"
    }
  }

  depends_on = [aws_eks_node_group.this]
}

resource "aws_iam_role" "ebs_csi" {
  name = "ebs-csi-pod-identity-${var.cluster_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
