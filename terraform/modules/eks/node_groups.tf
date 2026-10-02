locals {
  # kafka 用ノードは taint で隔離し、tolerations を持つ KafkaNodePool だけが乗れるようにする。
  # system には taint を打たない: tolerations 未指定の Pod は kafka ノードから自然に弾かれ system に
  # schedule されるため、system 側を taint で守る必要がない。
  dedicated_kafka_taints = [{ key = "DedicatedKafka", value = "true", effect = "NO_SCHEDULE" }]

  # broker / controller は役割ごとに最適なインスタンスサイズが異なり、1 ノード障害で両方を同時に
  # 失わないよう別 nodegroup に分ける。max は desired + 1（ローリング更新時に新ノードを起動する余裕）。
  node_groups = {
    "system-nodegroup" = {
      instance_type = var.system_node_group.instance_type
      min_size      = var.system_node_group.min_size
      max_size      = var.system_node_group.max_size
      desired_size  = var.system_node_group.desired_size
      role_label    = "system"
      taints        = []
    }
    "kafka-broker-nodegroup" = {
      instance_type = var.kafka_broker_node_group.instance_type
      min_size      = var.kafka_broker_node_group.count
      max_size      = var.kafka_broker_node_group.count + 1
      desired_size  = var.kafka_broker_node_group.count
      role_label    = "kafka-broker"
      taints        = local.dedicated_kafka_taints
    }
    "kafka-controller-nodegroup" = {
      instance_type = var.kafka_controller_node_group.instance_type
      min_size      = var.kafka_controller_node_group.count
      max_size      = var.kafka_controller_node_group.count + 1
      desired_size  = var.kafka_controller_node_group.count
      role_label    = "kafka-controller"
      taints        = local.dedicated_kafka_taints
    }
  }

  node_policy_names = ["AmazonEKSWorkerNodePolicy", "AmazonEKS_CNI_Policy", "AmazonEC2ContainerRegistryReadOnly"]

  node_role_policies = {
    for pair in setproduct(keys(local.node_groups), local.node_policy_names) :
    "${pair[0]}/${pair[1]}" => { group = pair[0], policy = pair[1] }
  }
}

resource "aws_iam_role" "node" {
  for_each = local.node_groups

  name = "${var.cluster_name}-${each.key}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = local.node_role_policies

  role       = aws_iam_role.node[each.value.group].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/${each.value.policy}"
}

resource "aws_eks_node_group" "this" {
  for_each = local.node_groups

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = each.key
  node_role_arn   = aws_iam_role.node[each.key].arn
  subnet_ids      = var.nodegroup_subnet_ids

  ami_type             = var.nodegroup_ami_type
  capacity_type        = var.nodegroup_capacity_type
  instance_types       = [each.value.instance_type]
  force_update_version = true

  labels = { role = each.value.role_label }

  scaling_config {
    min_size     = each.value.min_size
    max_size     = each.value.max_size
    desired_size = each.value.desired_size
  }

  node_repair_config {
    enabled = true
  }

  dynamic "taint" {
    for_each = each.value.taints

    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }

  # vpc-cni / kube-proxy が無いとノードが NotReady のままとなり、nodegroup の作成自体が
  # タイムアウトする。そのため nodegroup より前に Managed Addon として導入しておく。
  depends_on = [
    aws_iam_role_policy_attachment.node,
    aws_eks_addon.before_compute,
  ]
}
