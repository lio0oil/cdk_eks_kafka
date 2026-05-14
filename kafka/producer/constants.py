"""producer の動作設定値"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

# kafka/proto/ を import path に追加して、生成済み Protocol Buffer Python クラスを参照する
from event_pb2 import Event  # noqa: E402

# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
# 例: "kafka-bootstrap.example.internal:9094"
BOOTSTRAP_SERVERS = "localhost:9094"

DEFAULT_COUNT = 10
DEFAULT_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True)
class ProducerConfig:
    """1 つの送信先 Kafka topic と、そこに送るメッセージの生成関数をまとめる。

    topic: 送信先 Kafka topic。consumer 側 SchemaConfig.topic と一致させる
    make_payload: index (int) を受け取り、ProtoBuf SerializeToString() した bytes を返す
    """

    topic: str
    make_payload: Callable[[int], bytes]


def _make_event_payload(index: int) -> bytes:
    event = Event(
        id=index,
        datetime=datetime.now(UTC).isoformat(),
        name=f"hello from producer #{index}",
    )
    return event.SerializeToString()


# 送信対象トピックのリスト。各エントリの topic は consumer 側 SchemaConfig.topic と一致させる。
# サンプルは Event 1 種類のみ。新型を追加する場合は対応する ProtoBuf message クラスと
# make_payload 関数を用意して PRODUCERS に追記する。
PRODUCERS: list[ProducerConfig] = [
    ProducerConfig(
        topic="sample-events-event",
        make_payload=_make_event_payload,
    ),
    # 新型を追加する場合は ProducerConfig を 1 件追加 (例: Notification):
    # ProducerConfig(
    #     topic="sample-events-notification",
    #     make_payload=_make_notification_payload,
    # ),
]
