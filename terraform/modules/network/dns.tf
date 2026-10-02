# PrivateLink 消費側が自 VPC に同名の Private Hosted Zone を作れば、advertisedHost の再接続先も
# 同じ文字列で解決できる（提供側 VPC 内では NLB、消費側 VPC 内では消費側の VPC Endpoint）。
# ドメイン所有権の検証が要る Endpoint Service の Private DNS 名機能は使わない。
resource "aws_route53_zone" "kafka" {
  name = var.private_dns_name

  vpc {
    vpc_id = var.vpc_id
  }
}

resource "aws_route53_record" "kafka_nlb" {
  zone_id = aws_route53_zone.kafka.zone_id
  name    = var.private_dns_name
  type    = "A"

  alias {
    name                   = aws_lb.kafka.dns_name
    zone_id                = aws_lb.kafka.zone_id
    evaluate_target_health = false
  }
}
