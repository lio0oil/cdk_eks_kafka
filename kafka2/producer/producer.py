"""Kafka に Protocol Buffer メッセージを送信する producer。

送信先は constants.TOPIC の 1 topic 固定。consumer 側 SchemaConfig.topic と一致が必要。
"""

import argparse
import logging
import time
from itertools import count

from confluent_kafka import Producer

from constants import (
    BOOTSTRAP_SERVERS,
    DEFAULT_COUNT,
    DEFAULT_INTERVAL_SECONDS,
    TOPIC,
    make_envelope_payload,
)

logger = logging.getLogger("producer")


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "ekscdk-sample-producer",
        }
    )


def on_delivery(err, msg) -> None:
    if err is not None:
        logger.error("delivery failed: %s", err)
        return
    logger.info(
        "delivered topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def produce_one(bootstrap_servers: str, topic: str, index: int) -> None:
    """1 件だけ送信して flush() で配送を待つ。動作確認用。"""
    kafka_producer = build_producer(bootstrap_servers)
    kafka_producer.produce(
        topic,
        key=str(index).encode("utf-8"),
        value=make_envelope_payload(index),
        on_delivery=on_delivery,
    )
    remaining = kafka_producer.flush(timeout=10)
    if remaining > 0:
        raise RuntimeError(f"produce_one: flush 後も {remaining} 件未配送")


def run(
    producer: Producer,
    total: int | None,
    interval: float,
) -> int:
    """各 iteration で TOPIC に 1 件送信する。

    total は送信件数。total=None で無限モード。メッセージ番号 (= make_envelope_payload
    の index) は 0 始まり。
    """
    counter = count(0) if total is None else iter(range(total))
    sent = 0
    for i in counter:
        key = str(i).encode("utf-8")
        producer.produce(
            TOPIC,
            key=key,
            value=make_envelope_payload(i),
            on_delivery=on_delivery,
        )
        sent += 1
        # poll(0) で delivery callback を進める。flush() しないと配送結果が出ない。
        producer.poll(0)
        is_last = total is not None and i == total - 1
        if not is_last:
            time.sleep(interval)
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka に Protocol Buffer メッセージを送信する",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        help=f"送信件数 (default: {DEFAULT_COUNT})",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"iteration 間隔秒 (default: {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--infinite",
        action="store_true",
        help="無限に送信し続ける（--count は無視。Ctrl+C で停止）",
    )
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    producer = build_producer(BOOTSTRAP_SERVERS)
    total = None if args.infinite else args.count
    logger.info(
        "start: topic=%s servers=%s count=%s interval=%ss",
        TOPIC,
        BOOTSTRAP_SERVERS,
        "infinite" if total is None else total,
        args.interval,
    )
    sent = 0
    try:
        sent = run(producer, total, args.interval)
    except KeyboardInterrupt:
        logger.info("interrupted: 残メッセージを flush します")
    finally:
        # 未配送メッセージを送り切る。タイムアウトで諦める。
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("flush 後も %s 件未配送", remaining)
    logger.info("done: produce calls=%s", sent)


if __name__ == "__main__":
    main()
