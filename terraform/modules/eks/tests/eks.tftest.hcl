# command = apply は mock provider に対する適用で、実 AWS には触れない。
# IAM Role の ARN など computed 値の受け渡し（参照の配線）を検証するために apply を使う。

mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = {
      partition  = "aws"
      dns_suffix = "amazonaws.com"
    }
  }

  mock_resource "aws_eks_cluster" {
    defaults = {
      endpoint              = "https://0123456789ABCDEF.gr7.ap-northeast-1.eks.amazonaws.com"
      certificate_authority = [{ data = "bW9jaw==" }]
    }
  }

  # provider は ARN 引数の形式を検証するため、mock が生成する乱数文字列では通らない
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/mock"
    }
  }
}

# Role は参照の配線（どの Role がどのリソースに渡るか）を検証するため、
# インスタンスごとに異なる ARN を与える。
override_resource {
  target = aws_iam_role.cluster
  values = { arn = "arn:aws:iam::111111111111:role/cluster" }
}

override_resource {
  target = aws_iam_role.ebs_csi
  values = { arn = "arn:aws:iam::111111111111:role/ebs-csi" }
}

override_resource {
  target = aws_iam_role.node["system-nodegroup"]
  values = { arn = "arn:aws:iam::111111111111:role/node-system" }
}

override_resource {
  target = aws_iam_role.node["kafka-broker-nodegroup"]
  values = { arn = "arn:aws:iam::111111111111:role/node-broker" }
}

override_resource {
  target = aws_iam_role.node["kafka-controller-nodegroup"]
  values = { arn = "arn:aws:iam::111111111111:role/node-controller" }
}

variables {
  cluster_name         = "eks-cluster-dev"
  kubernetes_version   = "1.35"
  subnet_ids           = ["subnet-a", "subnet-b", "subnet-c"]
  nodegroup_subnet_ids = ["subnet-a"]
  admin_role_arn       = "arn:aws:iam::111111111111:role/eks-cluster-admin-eks-cluster-dev"

  deletion_protection       = false
  enable_control_plane_logs = false

  console_role_arns                           = []
  endpoint_public_access                      = true
  endpoint_private_access                     = true
  bootstrap_cluster_creator_admin_permissions = true
  control_plane_log_types                     = ["audit", "api", "authenticator"]
  upgrade_support_type                        = "STANDARD"
  nodegroup_capacity_type                     = "ON_DEMAND"

  addon_versions = {
    "aws-ebs-csi-driver"        = "v1.59.0-eksbuild.1"
    "metrics-server"            = "v0.8.1-eksbuild.6"
    "eks-node-monitoring-agent" = "v1.6.4-eksbuild.1"
    "snapshot-controller"       = "v8.5.0-eksbuild.3"
    "vpc-cni"                   = "v1.20.1-eksbuild.1"
    "coredns"                   = "v1.12.4-eksbuild.1"
    "kube-proxy"                = "v1.35.0-eksbuild.1"
  }

  nodegroup_ami_type = "AL2023_ARM_64_STANDARD"

  system_node_group = {
    instance_type = "t4g.large"
    min_size      = 2
    max_size      = 4
    desired_size  = 3
  }
  kafka_broker_node_group = {
    instance_type = "t4g.large"
    count         = 3
  }
  kafka_controller_node_group = {
    instance_type = "t4g.medium"
    count         = 3
  }
}

run "cluster_matches_legacy_settings" {
  command = apply

  assert {
    condition     = aws_eks_cluster.this.name == "eks-cluster-dev" && aws_eks_cluster.this.version == "1.35"
    error_message = "クラスター名 / バージョンが入力と一致しない"
  }

  assert {
    condition     = aws_eks_cluster.this.role_arn == aws_iam_role.cluster.arn
    error_message = "クラスターに専用の IAM Role が渡っていない"
  }

  assert {
    condition = (
      aws_eks_cluster.this.vpc_config[0].endpoint_private_access
      && aws_eks_cluster.this.vpc_config[0].endpoint_public_access
      && aws_eks_cluster.this.vpc_config[0].subnet_ids == toset(["subnet-a", "subnet-b", "subnet-c"])
    )
    error_message = "API endpoint が public/private 両方有効で、全 AZ の subnet を使う設定になっていない"
  }

  assert {
    condition = (
      aws_eks_cluster.this.access_config[0].authentication_mode == "API"
      && aws_eks_cluster.this.access_config[0].bootstrap_cluster_creator_admin_permissions
    )
    error_message = "認証モードが API で、作成者に admin を付与する設定になっていない"
  }

  # self-managed の vpc-cni / coredns / kube-proxy を自動導入させず、Managed Addon として明示管理する
  assert {
    condition     = aws_eks_cluster.this.bootstrap_self_managed_addons == false
    error_message = "bootstrap_self_managed_addons が false になっていない"
  }

  # Extended Support（追加課金）ではなく Standard Support に固定する
  assert {
    condition     = aws_eks_cluster.this.upgrade_policy[0].support_type == "STANDARD"
    error_message = "upgrade_policy が STANDARD になっていない"
  }

  assert {
    condition     = !aws_eks_cluster.this.deletion_protection && length(aws_eks_cluster.this.enabled_cluster_log_types) == 0
    error_message = "dev 相当の設定で削除保護 / コントロールプレーンログが無効になっていない"
  }
}

run "production_like_settings_enable_protection_and_logs" {
  command = apply

  variables {
    deletion_protection       = true
    enable_control_plane_logs = true
  }

  assert {
    condition     = aws_eks_cluster.this.deletion_protection
    error_message = "deletion_protection が反映されていない"
  }

  # controllerManager / scheduler はコスト対監査価値が低いため有効にしない
  assert {
    condition     = aws_eks_cluster.this.enabled_cluster_log_types == toset(["audit", "api", "authenticator"])
    error_message = "コントロールプレーンログが audit / api / authenticator の 3 種になっていない"
  }
}

run "cluster_role_is_assumed_by_eks_with_cluster_policy" {
  command = apply

  assert {
    condition     = jsondecode(aws_iam_role.cluster.assume_role_policy).Statement[0].Principal.Service == "eks.amazonaws.com"
    error_message = "クラスター Role の信頼先が eks.amazonaws.com ではない"
  }

  assert {
    condition     = aws_iam_role_policy_attachment.cluster.policy_arn == "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
    error_message = "クラスター Role に AmazonEKSClusterPolicy が付いていない"
  }
}

run "admin_role_gets_cluster_admin_access" {
  command = apply

  assert {
    condition     = toset(keys(aws_eks_access_entry.this)) == toset(["admin"])
    error_message = "既定では admin の Access Entry だけになっていない"
  }

  assert {
    condition = (
      aws_eks_access_entry.this["admin"].principal_arn == "arn:aws:iam::111111111111:role/eks-cluster-admin-eks-cluster-dev"
      && aws_eks_access_policy_association.cluster_admin["admin"].policy_arn == "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
      && aws_eks_access_policy_association.cluster_admin["admin"].access_scope[0].type == "cluster"
    )
    error_message = "admin Role に AmazonEKSClusterAdminPolicy がクラスタースコープで紐付いていない"
  }
}

run "console_roles_are_registered_as_additional_admins" {
  command = apply

  variables {
    console_role_arns = [
      "arn:aws:iam::111111111111:role/sso-a",
      "arn:aws:iam::111111111111:role/sso-b",
    ]
  }

  assert {
    condition     = toset(keys(aws_eks_access_entry.this)) == toset(["admin", "console-0", "console-1"])
    error_message = "console_role_arns が Access Entry として登録されていない"
  }

  assert {
    condition = (
      aws_eks_access_entry.this["console-1"].principal_arn == "arn:aws:iam::111111111111:role/sso-b"
      && length(aws_eks_access_policy_association.cluster_admin) == 3
    )
    error_message = "追加の管理者 Role に admin policy が紐付いていない"
  }
}

run "network_addons_are_installed_before_compute" {
  command = apply

  assert {
    condition     = toset(keys(aws_eks_addon.before_compute)) == toset(["vpc-cni", "kube-proxy"])
    error_message = "nodegroup より先に導入する Addon が vpc-cni / kube-proxy になっていない"
  }

  assert {
    condition = (
      aws_eks_addon.before_compute["vpc-cni"].addon_version == "v1.20.1-eksbuild.1"
      && aws_eks_addon.before_compute["kube-proxy"].addon_version == "v1.35.0-eksbuild.1"
    )
    error_message = "before_compute Addon のバージョンが addon_versions と一致しない"
  }
}

run "node_dependent_addons_are_installed_after_compute" {
  command = apply

  assert {
    condition = toset(keys(aws_eks_addon.after_compute)) == toset([
      "coredns",
      "eks-pod-identity-agent",
      "aws-ebs-csi-driver",
      "metrics-server",
      "eks-node-monitoring-agent",
      "snapshot-controller",
    ])
    error_message = "nodegroup 作成後に導入する Addon の一覧が期待と異なる"
  }

  assert {
    condition = (
      aws_eks_addon.after_compute["coredns"].addon_version == "v1.12.4-eksbuild.1"
      && aws_eks_addon.after_compute["metrics-server"].addon_version == "v0.8.1-eksbuild.6"
      && aws_eks_addon.after_compute["snapshot-controller"].addon_version == "v8.5.0-eksbuild.3"
      && aws_eks_addon.after_compute["aws-ebs-csi-driver"].addon_version == "v1.59.0-eksbuild.1"
      && aws_eks_addon.after_compute["eks-node-monitoring-agent"].addon_version == "v1.6.4-eksbuild.1"
    )
    error_message = "after_compute Addon のバージョンが addon_versions と一致しない"
  }
}

# --verbosity は zap level に -1 倍して渡される。WARN 以上にするには -1。
# additionalArgs は完全置換なので、chart 既定の --metrics-address も含める必要がある。
run "node_monitoring_agent_logs_warn_and_above" {
  command = apply

  assert {
    condition = jsondecode(aws_eks_addon.after_compute["eks-node-monitoring-agent"].configuration_values) == {
      nodeAgent = { additionalArgs = ["--metrics-address=:8003", "--verbosity=-1"] }
    }
    error_message = "eks-node-monitoring-agent の additionalArgs が期待と異なる"
  }
}

run "ebs_csi_driver_uses_pod_identity_role" {
  command = apply

  assert {
    condition = (
      one(aws_eks_addon.after_compute["aws-ebs-csi-driver"].pod_identity_association).service_account == "ebs-csi-controller-sa"
      && one(aws_eks_addon.after_compute["aws-ebs-csi-driver"].pod_identity_association).role_arn == aws_iam_role.ebs_csi.arn
    )
    error_message = "EBS CSI Addon が ebs-csi-controller-sa に専用 Role を Pod Identity で紐付けていない"
  }

  assert {
    condition     = aws_iam_role.ebs_csi.name == "ebs-csi-pod-identity-eks-cluster-dev"
    error_message = "EBS CSI Role の名前が ebs-csi-pod-identity-<cluster_name> ではない"
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.ebs_csi.assume_role_policy).Statement[0].Principal.Service == "pods.eks.amazonaws.com"
      && toset(jsondecode(aws_iam_role.ebs_csi.assume_role_policy).Statement[0].Action) == toset(["sts:AssumeRole", "sts:TagSession"])
    )
    error_message = "EBS CSI Role が pods.eks.amazonaws.com に AssumeRole + TagSession を許可していない"
  }

  assert {
    condition     = aws_iam_role_policy_attachment.ebs_csi.policy_arn == "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
    error_message = "EBS CSI Role に AmazonEBSCSIDriverPolicy が付いていない"
  }
}

run "three_node_groups_with_common_settings" {
  command = apply

  assert {
    condition     = toset(keys(aws_eks_node_group.this)) == toset(["system-nodegroup", "kafka-broker-nodegroup", "kafka-controller-nodegroup"])
    error_message = "system / kafka-broker / kafka-controller の 3 nodegroup になっていない"
  }

  assert {
    condition = alltrue([
      for k, ng in aws_eks_node_group.this :
      ng.cluster_name == "eks-cluster-dev"
      && ng.ami_type == "AL2023_ARM_64_STANDARD"
      && ng.capacity_type == "ON_DEMAND"
      && ng.subnet_ids == toset(["subnet-a"])
      && ng.node_repair_config[0].enabled
      && ng.force_update_version
    ])
    error_message = "nodegroup の共通設定（AMI / ON_DEMAND / subnet / 自動修復）が期待と異なる"
  }

  assert {
    condition = alltrue([
      for k, ng in aws_eks_node_group.this : ng.node_role_arn == aws_iam_role.node[k].arn
    ])
    error_message = "nodegroup が自身専用の Node Role を使っていない"
  }
}

run "system_node_group_has_no_taint" {
  command = apply

  assert {
    condition = (
      aws_eks_node_group.this["system-nodegroup"].instance_types == tolist(["t4g.large"])
      && aws_eks_node_group.this["system-nodegroup"].scaling_config[0].min_size == 2
      && aws_eks_node_group.this["system-nodegroup"].scaling_config[0].max_size == 4
      && aws_eks_node_group.this["system-nodegroup"].scaling_config[0].desired_size == 3
      && aws_eks_node_group.this["system-nodegroup"].labels == tomap({ role = "system" })
    )
    error_message = "system nodegroup のサイズ / instance type / label が期待と異なる"
  }

  # kafka nodegroup だけを DedicatedKafka taint で隔離し、system 側は taint で守らない
  assert {
    condition     = length(aws_eks_node_group.this["system-nodegroup"].taint) == 0
    error_message = "system nodegroup に taint が付いている"
  }
}

run "kafka_node_groups_are_isolated_by_taint_and_sized_by_count" {
  command = apply

  assert {
    condition = (
      aws_eks_node_group.this["kafka-broker-nodegroup"].instance_types == tolist(["t4g.large"])
      && aws_eks_node_group.this["kafka-broker-nodegroup"].labels == tomap({ role = "kafka-broker" })
      && aws_eks_node_group.this["kafka-controller-nodegroup"].instance_types == tolist(["t4g.medium"])
      && aws_eks_node_group.this["kafka-controller-nodegroup"].labels == tomap({ role = "kafka-controller" })
    )
    error_message = "broker / controller nodegroup の instance type / label が期待と異なる"
  }

  assert {
    condition = alltrue([
      for k in ["kafka-broker-nodegroup", "kafka-controller-nodegroup"] :
      toset([for t in aws_eks_node_group.this[k].taint : "${t.key}=${t.value}:${t.effect}"]) == toset(["DedicatedKafka=true:NO_SCHEDULE"])
    ])
    error_message = "Kafka nodegroup に DedicatedKafka=true:NO_SCHEDULE の taint が付いていない"
  }

  # ローリング更新時に新ノードを起動できるよう max は desired + 1
  assert {
    condition = alltrue([
      for k in ["kafka-broker-nodegroup", "kafka-controller-nodegroup"] :
      aws_eks_node_group.this[k].scaling_config[0].min_size == 3
      && aws_eks_node_group.this[k].scaling_config[0].desired_size == 3
      && aws_eks_node_group.this[k].scaling_config[0].max_size == 4
    ])
    error_message = "Kafka nodegroup が min=desired=count、max=count+1 になっていない"
  }
}

run "broker_node_group_follows_broker_count" {
  command = apply

  variables {
    kafka_broker_node_group = {
      instance_type = "t4g.large"
      count         = 5
    }
  }

  assert {
    condition = (
      aws_eks_node_group.this["kafka-broker-nodegroup"].scaling_config[0].min_size == 5
      && aws_eks_node_group.this["kafka-broker-nodegroup"].scaling_config[0].desired_size == 5
      && aws_eks_node_group.this["kafka-broker-nodegroup"].scaling_config[0].max_size == 6
    )
    error_message = "broker 数の変更が nodegroup の capacity に反映されていない"
  }
}

run "node_roles_are_assumed_by_ec2_with_worker_policies" {
  command = apply

  assert {
    condition = alltrue([
      for r in values(aws_iam_role.node) :
      jsondecode(r.assume_role_policy).Statement[0].Principal.Service == "ec2.amazonaws.com"
    ])
    error_message = "Node Role の信頼先が ec2.amazonaws.com ではない"
  }

  assert {
    condition = toset([for a in aws_iam_role_policy_attachment.node : a.policy_arn]) == toset([
      "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
      "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
      "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
    ]) && length(aws_iam_role_policy_attachment.node) == 9
    error_message = "各 Node Role に Worker / CNI / ECR ReadOnly の 3 policy が付いていない"
  }
}

run "outputs_expose_cluster_connection_info" {
  command = apply

  assert {
    condition     = output.cluster_name == "eks-cluster-dev"
    error_message = "cluster_name 出力が不正"
  }
}

run "addon_versions_missing_required_addon_is_rejected" {
  command = plan

  variables {
    addon_versions = {
      "vpc-cni"    = "v1.20.1-eksbuild.1"
      "kube-proxy" = "v1.35.0-eksbuild.1"
    }
  }

  expect_failures = [var.addon_versions]
}

# API endpoint の公開範囲・作成者への admin 付与・ログ種別・サポート種別・購入オプションは、
# module に値を持たず呼び出し側（env）が決める（stg/prd は private のみ・作成者 admin 無し等に変えうる）
run "cluster_settings_come_from_variables" {
  command = apply

  variables {
    endpoint_public_access                      = false
    endpoint_private_access                     = true
    bootstrap_cluster_creator_admin_permissions = false
    enable_control_plane_logs                   = true
    control_plane_log_types                     = ["audit"]
    upgrade_support_type                        = "EXTENDED"
    nodegroup_capacity_type                     = "SPOT"
  }

  assert {
    condition = (
      !aws_eks_cluster.this.vpc_config[0].endpoint_public_access
      && aws_eks_cluster.this.vpc_config[0].endpoint_private_access
      && !aws_eks_cluster.this.access_config[0].bootstrap_cluster_creator_admin_permissions
    )
    error_message = "API endpoint の公開範囲 / 作成者への admin 付与が variables から決まっていない"
  }

  assert {
    condition     = aws_eks_cluster.this.enabled_cluster_log_types == toset(["audit"]) && aws_eks_cluster.this.upgrade_policy[0].support_type == "EXTENDED"
    error_message = "ログ種別 / サポート種別が variables から決まっていない"
  }

  assert {
    condition     = alltrue([for ng in values(aws_eks_node_group.this) : ng.capacity_type == "SPOT"])
    error_message = "nodegroup の購入オプションが variables から決まっていない"
  }
}
