variable "vpc_id" {
  type        = string
  description = "踏み台を置く VPC"
}

variable "subnet_id" {
  type        = string
  description = "踏み台を置く private subnet"
}

variable "kubernetes_version" {
  type        = string
  description = "EKS と同じ minor の kubectl を入れるための Kubernetes バージョン（例: 1.35）。minor が 1 つ違うまでは互換"
}

variable "instance_type" {
  type        = string
  description = "踏み台の instance type"
}
