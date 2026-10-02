region = "ap-northeast-1"

# dev は環境破棄時に PVC ごとクリーンアップする（stg/prd は false でデータを保護する）
kafka_delete_claim = true

# stg/prd 運用前に見直しが必要な暫定値（.claude/TODO.md 参照）
kafka_broker_resources = {
  memory_request = "2Gi"
  memory_limit   = "4Gi"
  cpu_request    = "250m"
  cpu_limit      = "500m"
  storage_size   = "20Gi"
  jvm_xms        = "1024m"
  jvm_xmx        = "2048m"
}
kafka_controller_resources = {
  memory_request = "1Gi"
  memory_limit   = "2Gi"
  cpu_request    = "250m"
  cpu_limit      = "500m"
  storage_size   = "20Gi"
  jvm_xms        = "512m"
  jvm_xmx        = "1024m"
}

# Kafka のデフォルト値（log.retention.hours=168h=7日）を明示化した値
kafka_topic_retention_ms = "604800000"
