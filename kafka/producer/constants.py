"""producer の動作設定値"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

# kafka/producer/event_pb2.py / metric_pb2.py は kafka/proto/*.proto から生成した Python クラス。
from event_pb2 import Envelope, OrderEvent, UserEvent  # noqa: E402  # pyright: ignore[reportAttributeAccessIssue]
from metric_pb2 import Metric  # noqa: E402  # pyright: ignore[reportAttributeAccessIssue]

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


def _make_metric_payload(index: int) -> bytes:
    """Metric を 1 つ作って serialize する。

    Metric は Envelope (event.proto) とは別構造のフラットな proto (metric.proto)。
    同じ topic に Envelope と相乗りで流す。consumer は Kafka header の proto-schema
    ("Metric") でこの型を識別し、metric_id / observed_at / kind を Envelope と同じ
    event_id / event_datetime / extra_type カラムへ写像する。kind は extra_type に
    入る低カーディナリティの種別文字列。
    """
    return Metric(
        metric_id=index,
        observed_at=datetime.now(UTC).isoformat(),
        kind="cpu" if index % 2 == 0 else "memory",
        host=f"host-{index % 3}",
        value=float(index),
    ).SerializeToString()


# 送信対象のリスト。1 つの topic "sample-events-event" に構造の異なる 2 つの ProtoBuf
# 定義 (Envelope / Metric) を相乗りで流すサンプル。各 iteration で両方を produce し、
# Kafka header の proto-schema で型を伝える。consumer は header を見て from_protobuf を
# 出し分け、同一テーブル sample_events_event に書く。
PRODUCERS: list[ProducerConfig] = [
    ProducerConfig(
        topic="sample-events-event",
        make_payload=_make_envelope_payload,
        schema_name="Envelope",
        schema_version=1,
    ),
    ProducerConfig(
        topic="sample-events-event",
        make_payload=_make_metric_payload,
        schema_name="Metric",
        schema_version=1,
    ),
]
