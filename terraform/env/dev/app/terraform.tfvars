region      = "ap-northeast-1"
environment = "dev"

log_retention_days = 7
# dev は環境破棄を容易にする。stg/prd は false で DLQ とロググループを残す
log_force_destroy = true

# Lambda コンシューマ。ログレベルは ALC で制御する（ハンドラ側で固定しない）
lambda_runtime               = "python3.13"
lambda_timeout_seconds       = 30
lambda_application_log_level = "INFO"

# ESM。バッチウィンドウは既定の 500ms だと呼び出し回数が増えやすいため 1 秒に伸ばす
# （一度変更すると既定には ESM の再作成でしか戻せない）
esm_starting_position           = "LATEST"
esm_batch_size                  = 100
esm_max_batching_window_seconds = 1

# kafka_consumer_code_object_version は、infra の適用後に function.zip をアップロードして得た S3 VersionId を
# ここに書く（アップロードするまで値が無いため、最初は書かない）。コミットすればデプロイされたコードの履歴が
# git に残り、ロールバックは前の VersionId に戻して適用するだけになる。
# kafka_consumer_code_object_version = "<VersionId>"
