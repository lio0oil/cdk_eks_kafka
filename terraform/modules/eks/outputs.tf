# 他の module はこの module 全体への depends_on で、nodegroup と全 Addon（coredns /
# eks-pod-identity-agent を含む）の作成完了を待てる。

output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  value = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_security_group_id" {
  description = "EKS が自動作成するクラスター SG。ノードの ENI もこの SG を使う"
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}
