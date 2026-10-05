output "test_topic_name" {
  description = "test-topic の名前。app 層（Lambda の ESM）が購読する topic として使う"
  value       = module.kafka.test_topic_name
}
