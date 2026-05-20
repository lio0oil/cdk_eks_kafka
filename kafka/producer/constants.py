"""producer の動作設定値"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

# kafka/producer/event_pb2.py は kafka/proto/event.proto から生成した Python クラス。
from event_pb2 import Envelope, OrderEvent, UserEvent  # noqa: E402  # pyright: ignore[reportAttributeAccessIssue]

# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
# 例: "kafka-bootstrap.example.internal:9094"
BOOTSTRAP_SERVERS = "localhost:9094"

DEFAULT_COUNT = 10
DEFAULT_INTERVAL_SECONDS = 1.0

# ProtoBuf schema を伝達する Kafka header key。consumer 側 (constants.py) と一致させる。
PROTO_VERSION_HEADER_KEY = "proto-version"
PROTO_SCHEMA_HEADER_KEY = "proto-schema"


@dataclass(frozen=True)
class ProducerConfig:
    """1 つの送信先 Kafka topic と、そこに送るメッセージの生成関数をまとめる。

    topic: 送信先 Kafka topic。consumer 側 SchemaConfig.topic と一致させる
    make_payload: index (int) を受け取り、ProtoBuf SerializeToString() した bytes を返す
    schema_name: ProtoBuf message 型名 (= "Envelope")。Kafka header で消費側に伝える
    schema_version: ProtoBuf スキーマの major version。互換性が壊れる変更
        (Envelope のフィールド型変更等) を行う時にだけ上げる
    """

    topic: str
    make_payload: Callable[[int], bytes]
    schema_name: str
    schema_version: int


def _make_envelope_payload(index: int) -> bytes:
    """Envelope を 1 つ作って serialize する。

    Event は必ず埋め、extra は index % 2 で OrderEvent / UserEvent を交互に切り替える。
    consumer は extra の oneof case で行を振り分け、どの case にも入らない
    (未知 extra) 行は DLQ に送る。
    """
    envelope = Envelope()
    envelope.event.id = index
    envelope.event.datetime = datetime.now(UTC).isoformat()
    envelope.event.name = f"hello from producer #{index}"
    if index % 2 == 0:
        envelope.order_event.CopyFrom(OrderEvent(id=index, ordername=f"order-{index}"))
    else:
        envelope.user_event.CopyFrom(UserEvent(id=index, eventname=f"user-{index}"))
    return envelope.SerializeToString()


# 送信対象トピックのリスト。Envelope 採用後は 1 topic に集約される。
# 新しい extra 型を追加する場合は event.proto の Envelope.extra に oneof case を増やし、
# _make_envelope_payload の振り分けロジックを拡張する (PRODUCERS は触らない)。
PRODUCERS: list[ProducerConfig] = [
    ProducerConfig(
        topic="sample-events-event",
        make_payload=_make_envelope_payload,
        schema_name="Envelope",
        schema_version=1,
    ),
]
