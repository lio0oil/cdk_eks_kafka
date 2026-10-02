region      = "ap-northeast-1"
environment = "dev"

table_bucket_name = "kafka-events-dev"

# 実名は kafka-consumer-checkpoint-<account>-<suffix>（アカウントと環境でグローバル衝突を避ける）
checkpoint_bucket_suffix = "dev"

# dev は環境破棄を容易にするため中身ごと削除可能にする（stg/prd は false でデータを保護する）
force_destroy = true

# アカウント + リージョン内で一意が必要なため、環境ごとに変える
kms_key_alias = "alias/kafka-consumer-s3tables-dev"
