import base64
import hashlib
import io
import logging
import os
from datetime import UTC, datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from decode import InvalidRecord, decode_message
from schema import DLQ_SCHEMA, load_schema

logger = logging.getLogger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
DEFAULT_MAX_DECOMPRESSED_BYTES = 10 * 1024 * 1024
DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "staging_schema.json"


@cache
def s3_client() -> Any:
    return boto3.client("s3")


def utcnow() -> datetime:
    return datetime.now(UTC)


def lambda_handler(event: dict[str, Any], context: Any) -> None:
    handle(
        event,
        s3=s3_client(),
        bucket=os.environ["STAGING_BUCKET"],
        schema=load_schema(DEFAULT_SCHEMA_PATH),
        now=utcnow(),
        max_decompressed_bytes=DEFAULT_MAX_DECOMPRESSED_BYTES,
    )


def handle(
    event: dict[str, Any],
    *,
    s3: Any,
    bucket: str,
    schema: pa.Schema,
    now: datetime,
    max_decompressed_bytes: int,
) -> None:
    written_at = (now - EPOCH) // timedelta(microseconds=1)
    rows: list[dict[str, Any]] = []
    dlq_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    offsets: dict[tuple[str, int], tuple[int, int]] = {}

    for partition_key, records in event["records"].items():
        topic, _, partition_text = partition_key.rpartition("-")
        partition = int(partition_text)
        for record in records:
            offset = record["offset"]
            low, high = offsets.get((topic, partition), (offset, offset))
            offsets[(topic, partition)] = (min(low, offset), max(high, offset))

            payload = base64.b64decode(record["value"])
            try:
                message = decode_message(payload, max_decompressed_bytes)
            except InvalidRecord as e:
                dlq_rows.append(
                    {"topic": topic, "partition": partition, "offset": offset, "reason": e.reason, "payload": payload}
                )
                continue

            identity = (message.device_id, message.message_id)
            if identity in seen:
                continue
            seen.add(identity)
            rows.extend(
                {
                    "device_id": message.device_id,
                    "message_id": message.message_id,
                    "device_code": message.device_code,
                    "sent_at": message.sent_at,
                    "measured_at": reading.measured_at,
                    "metric": reading.metric,
                    "value": reading.value,
                    "written_at": written_at,
                }
                for reading in message.readings
            )

    minute = now.strftime("%Y%m%dT%H%M")
    batch_id = _batch_id(offsets)
    if rows:
        _put_parquet(s3, bucket, f"incoming/t={minute}/{batch_id}.parquet", pa.Table.from_pylist(rows, schema=schema))
    if dlq_rows:
        _put_parquet(
            s3, bucket, f"dlq/t={minute}/{batch_id}.parquet", pa.Table.from_pylist(dlq_rows, schema=DLQ_SCHEMA)
        )
    logger.info("batch %s: %d rows, %d dlq records", batch_id, len(rows), len(dlq_rows))


def _batch_id(offsets: dict[tuple[str, int], tuple[int, int]]) -> str:
    canonical = ";".join(
        f"{topic}-{partition}:{low}-{high}" for (topic, partition), (low, high) in sorted(offsets.items())
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


def _put_parquet(s3: Any, bucket: str, key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
