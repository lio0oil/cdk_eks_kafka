output "test_topic_name" {
  description = "test-topic の名前。購読側（app 層の Lambda の ESM）が使う"
  value       = kubernetes_manifest.test_topic.manifest.metadata.name
}
