locals {
  nlb_ports_by_name = { for p in var.nlb_ports : p.name => p }
}

resource "aws_security_group" "kafka_nlb" {
  name        = "${var.cluster_name}-kafka-nlb"
  description = "Kafka shared NLB"
  vpc_id      = module.vpc.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "kafka_nlb" {
  for_each = local.nlb_ports_by_name

  security_group_id = aws_security_group.kafka_nlb.id
  description       = "from ${module.vpc.vpc_cidr_block}:${each.value.listener_port}"
  cidr_ipv4         = module.vpc.vpc_cidr_block
  ip_protocol       = "tcp"
  from_port         = each.value.listener_port
  to_port           = each.value.listener_port
}

resource "aws_vpc_security_group_egress_rule" "kafka_nlb_all" {
  security_group_id = aws_security_group.kafka_nlb.id
  description       = "Allow all outbound traffic by default"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# 再作成すると ARN が変わり PrivateLink が壊れるため、この NLB の変更は慎重に行う。
# kafka_single_az=true では NLB も Kafka nodegroup と同じ 1 AZ に揃える。NLB が multi-AZ のまま
# ターゲットだけ 1 AZ に寄ると他 AZ の NLB ノードから到達するのに cross-zone が必須になり、
# AZ 跨ぎのデータ転送料が発生するため。
resource "aws_lb" "kafka" {
  name                             = var.nlb_name
  load_balancer_type               = "network"
  internal                         = true
  security_groups                  = [aws_security_group.kafka_nlb.id]
  subnets                          = var.kafka_single_az ? slice(module.vpc.private_subnets, 0, 1) : module.vpc.private_subnets
  enable_cross_zone_load_balancing = !var.kafka_single_az
  enable_deletion_protection       = false
}

# ターゲット登録は AWS Load Balancer Controller の TargetGroupBinding が Service の
# EndpointSlice と同期して行う。target_type=instance だと broker のいない同 nodegroup 内の
# 他ノードまで登録されてしまうため、Pod IP を直接登録できる ip にしている。
# port は broker ごとの NodePort ではなく、全 broker 共通の external listener の内部 port。
# ip + TCP は client IP preservation が既定で無効なため、明示的に有効化する。
resource "aws_lb_target_group" "kafka" {
  for_each = local.nlb_ports_by_name

  name               = "${var.nlb_name}-${lower(each.key)}"
  port               = var.kafka_target_port
  protocol           = "TCP"
  target_type        = "ip"
  vpc_id             = module.vpc.vpc_id
  preserve_client_ip = "true"

  health_check {
    protocol            = "TCP"
    port                = tostring(var.kafka_target_port)
    healthy_threshold   = var.target_group_health_check.healthy_threshold
    unhealthy_threshold = var.target_group_health_check.unhealthy_threshold
    interval            = var.target_group_health_check.interval
  }
}

resource "aws_lb_listener" "kafka" {
  for_each = local.nlb_ports_by_name

  load_balancer_arn = aws_lb.kafka.arn
  port              = each.value.listener_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.kafka[each.key].arn
  }
}

resource "aws_vpc_endpoint_service" "kafka" {
  acceptance_required        = false
  network_load_balancer_arns = [aws_lb.kafka.arn]

  tags = {
    Name = "kafka-endpoint-service"
  }
}
