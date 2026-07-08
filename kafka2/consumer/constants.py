"""consumer の動作設定値。

Kafka に流れる proto は Envelope 1 種類のみ。Envelope.content.historyrecords
(repeated HistoryRecord) は classification 毎に 1 レコードへ分解し、Content を
「選択した HistoryRecord のみ残したもの」に絞って S3 に parquet で書く
(consumer.py の _write_valid 参照)。
"""

import os
from pathlib import Path

# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
BOOTSTRAP_SERVERS = "localhost:9094"

# 初回起動時のみ参照される startingOffsets。2 回目以降は checkpointLocation の値が優先される。
DEFAULT_STARTING_OFFSETS = "earliest"

# Kafka source の minPartitions。Kafka topic の実パーティション数より大きい値を指定すると、
# 1 Kafka partition を複数の Spark task に分割して読み、並列度を上げられる。
MIN_PARTITIONS = 4

# Spark の from_protobuf 第 3 引数に渡す FileDescriptorSet (kafka2/proto/events.desc)。
# 実環境では S3 URI に置き換える。例: "s3://<アーティファクトバケット>/proto/events.desc"
DESCRIPTOR_FILE = str(Path(__file__).resolve().parent / "events.desc")

# 処理対象 Kafka topic。producer 側 (constants.py の TOPIC) と一致させる。
TOPIC = "sample-events-event"

# Spark の from_protobuf 第 2 引数に渡す package.message。
PROTOBUF_FULL_NAME = "ekscdk.kafka2.Envelope"

# DLQ の reason 列に入れる分類値。集計・アラート設定で参照する。
DLQ_REASON_ZLIB_ERROR = "zlib_decompress_error"  # producer が付与した zlib 圧縮を展開できなかった
DLQ_REASON_DESERIALIZE_ERROR = "deserialize_error"  # (zlib 展開後の bytes を) from_protobuf がデコードできなかった

# checkpointLocation / 出力 parquet 用 S3 バケット名。バケット名にアカウント ID と env 名が
# 含まれるためリポジトリには持たず .env 経由で受ける (launch.json の envFile で読み込む)。
CHECKPOINT_BUCKET = os.environ["CHECKPOINT_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]

# Structured Streaming の checkpointLocation。
CHECKPOINT_LOCATION = f"s3a://{CHECKPOINT_BUCKET}/envelope/"

# classification / year / month / day で partition した parquet の書き込み先ルート。
OUTPUT_PATH = f"s3a://{OUTPUT_BUCKET}/sample-events-event/"

# DLQ 行きデータの退避先。year / month / day で partition する
# (reason は分析対象ではないため partition column に含めない)。
DLQ_OUTPUT_PATH = f"s3a://{OUTPUT_BUCKET}/sample-events-dlq/"
