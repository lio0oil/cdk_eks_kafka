"""JSON/YAML ファイルの内容を Envelope にバインドして Kafka に送信する動作確認用スクリプト。

producer.py の make_envelope_payload は連番から機械的に生成した値しか送れないため、
任意のテストデータ (境界値・異常系など) を送りたいときに使う。ファイル側のキー名は
event.proto のフィールド名 (id/datetime/userid/username/historyid/memo/classification) と
一致させる必要がある (ParseDict がそのまま検証する)。
"""

import argparse
import json
import logging
import zlib
from pathlib import Path
from typing import Any

import yaml
from google.protobuf.json_format import ParseDict

from constants import BOOTSTRAP_SERVERS, TOPIC
from event_pb2 import Envelope  # pyright: ignore[reportAttributeAccessIssue]
from producer import build_producer, on_delivery

logger = logging.getLogger("send_from_file")

_YAML_SUFFIXES = (".yaml", ".yml")


def load_envelope_dict(path: Path) -> dict[str, Any]:
    """拡張子で JSON/YAML を判定してファイルを dict として読み込む。"""
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        return json.loads(text)
    if path.suffix in _YAML_SUFFIXES:
        return yaml.safe_load(text)
    raise ValueError(f"拡張子 {path.suffix} は未対応 (.json / .yaml / .yml のみ): {path}")


def build_payload_from_dict(data: dict[str, Any]) -> bytes:
    """dict を Envelope にバインドし、SerializeToString + zlib 圧縮する。

    consumer 側が zlib 展開を前提にしている (kafka2/consumer/consumer.py) ため、
    constants.make_envelope_payload と同じ圧縮方式に揃える。
    """
    envelope = ParseDict(data, Envelope())
    return zlib.compress(envelope.SerializeToString())


def send_from_file(bootstrap_servers: str, topic: str, path: Path, key: str) -> None:
    """ファイルを読み込んで 1 件だけ送信し、flush() で配送を待つ。"""
    payload = build_payload_from_dict(load_envelope_dict(path))
    kafka_producer = build_producer(bootstrap_servers)
    kafka_producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=payload,
        on_delivery=on_delivery,
    )
    remaining = kafka_producer.flush(timeout=10)
    if remaining > 0:
        raise RuntimeError(f"send_from_file: flush 後も {remaining} 件未配送")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JSON/YAML ファイルを Envelope にバインドして Kafka に送信する",
    )
    parser.add_argument("file", type=Path, help="送信する JSON/YAML ファイルのパス")
    parser.add_argument("--key", default="0", help="Kafka メッセージキー (default: 0)")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    logger.info(
        "start: file=%s topic=%s servers=%s",
        args.file,
        args.topic,
        args.bootstrap_servers,
    )
    send_from_file(args.bootstrap_servers, args.topic, args.file, args.key)
    logger.info("done")


if __name__ == "__main__":
    main()
