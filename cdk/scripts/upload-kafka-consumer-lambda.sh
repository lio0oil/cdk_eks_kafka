#!/usr/bin/env bash
# cdk/lambda/kafka_consumer/ を決定論的に zip 化し、KafkaConsumerInfraConstruct
# (EksCdkStack) が作るアーティファクトバケットへアップロードする。
#
# 出力される VersionId を ClusterConfig.kafka_consumer_code_object_version
# (cdk/ekscdk/config.py) に設定してから KafkaConsumerAppStack をデプロイすること。
#
# 使い方:
#   bash scripts/upload-kafka-consumer-lambda.sh <bucket-name>
#
# <bucket-name> は EksCdkStack デプロイ後、以下で確認できる:
#   aws cloudformation describe-stacks --stack-name EksCdkStack \
#     --query "Stacks[0].Outputs"
#
# 前提: AWS CLI に認証済みであること。cdk/ ディレクトリで実行すること（uv run のため）。
set -euo pipefail

# kafka_consumer_app_stack.py の ARTIFACT_CODE_KEY と一致させること。
KEY="function.zip"

if [ $# -ne 1 ]; then
  echo "usage: $0 <bucket-name>" >&2
  exit 1
fi
BUCKET="$1"

ZIP_PATH=$(uv run python -c "
from ekscdk.constructs.kafka_consumer_infra import build_lambda_zip, LAMBDA_ASSET_DIR, LAMBDA_ZIP_PATH
print(build_lambda_zip(LAMBDA_ASSET_DIR, LAMBDA_ZIP_PATH))
")

VERSION_ID=$(aws s3api put-object \
  --bucket "${BUCKET}" \
  --key "${KEY}" \
  --body "${ZIP_PATH}" \
  --query 'VersionId' --output text)

echo "Uploaded ${ZIP_PATH} to s3://${BUCKET}/${KEY}"
echo "VersionId: ${VERSION_ID}"
echo
echo "ClusterConfig.kafka_consumer_code_object_version にこの VersionId を設定してから"
echo "KafkaConsumerAppStack をデプロイしてください。"
