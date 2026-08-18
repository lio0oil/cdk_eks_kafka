"""producer の動作設定値"""

import zlib
from datetime import UTC, datetime

# kafka2/producer/event_pb2.py は kafka2/proto/event.proto から生成した Python クラス。
from event_pb2 import Envelope  # noqa: E402  # pyright: ignore[reportAttributeAccessIssue]

# Envelope.content.historyrecords に詰める件数 (合計 18 件)
HISTORY_RECORD_COUNT = 18

# classification の種類数 (classification-0 〜 classification-4 の 5 種類をループさせる)
CLASSIFICATION_COUNT = 5

# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
# 例: "kafka-bootstrap.example.internal:9094"
BOOTSTRAP_SERVERS = "localhost:9094"

# 送信先 Kafka topic。consumer 側 constants.TOPIC と一致させる
TOPIC = "sample-events-event"

DEFAULT_COUNT = 10
DEFAULT_INTERVAL_SECONDS = 1.0


def build_envelope(index: int) -> Envelope:
    """Envelope を 1 つ作る。

    common / user は必ず埋め、content.historyrecords には HISTORY_RECORD_COUNT 件の
    HistoryRecord を詰める。
    """
    now = datetime.now(UTC).isoformat()
    envelope = Envelope()
    envelope.common.id = index
    envelope.common.datetime = now
    envelope.user.userid = index
    envelope.user.username = f"user-{index}"
    for i in range(HISTORY_RECORD_COUNT):
        record = envelope.content.historyrecords.add()
        record.historyid = index * HISTORY_RECORD_COUNT + i
        record.datetime = now
        record.memo = f"memo-{index}-{i}"
        record.classification = f"classification-{i % CLASSIFICATION_COUNT}"
    return envelope


def make_envelope_payload(index: int) -> bytes:
    """Envelope を作って serialize し、zlib 圧縮して返す。

    consumer 側は from_protobuf に渡す前に zlib 展開する (kafka2/consumer/consumer.py 参照)。
    """
    return zlib.compress(build_envelope(index).SerializeToString())
