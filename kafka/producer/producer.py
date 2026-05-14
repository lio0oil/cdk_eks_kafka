"""Kafka に Protocol Buffer メッセージを送信する producer。

送信対象トピックと生成関数の組は constants.PRODUCERS で定義する。各 iteration で
全 PRODUCERS に対し produce する構造。consumer 側 SchemaConfig.topic と一致が必要。
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
    PRODUCERS,
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


def run(
    producer: Producer,
    total: int | None,
    interval: float,
) -> int:
    """各 iteration で PRODUCERS 全てに送信する。

    total は iteration 数 (= 各 topic への送信件数)。総 produce 呼び出し数は
    total * len(PRODUCERS) になる。total=None で無限モード。
    """
    counter = count(1) if total is None else iter(range(1, total + 1))
    sent = 0
    for i in counter:
        key = str(i).encode("utf-8")
        for pcfg in PRODUCERS:
            producer.produce(
                pcfg.topic,
                key=key,
                value=pcfg.make_payload(i),
                on_delivery=on_delivery,
            )
            sent += 1
        # poll(0) で delivery callback を進める。flush() しないと配送結果が出ない。
        producer.poll(0)
        is_last = total is not None and i == total
        if not is_last:
            time.sleep(interval)
    return sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka に Protocol Buffer メッセージを送信する (PRODUCERS の全 topic 宛)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        help=f"iteration 数 (= 各 topic への送信件数。default: {DEFAULT_COUNT})",
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
    topics = [p.topic for p in PRODUCERS]
    logger.info(
        "start: topics=%s servers=%s count=%s interval=%ss",
        topics,
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
    logger.info("done: produce calls=%s (iterations * %s topics)", sent, len(PRODUCERS))


if __name__ == "__main__":
    main()
