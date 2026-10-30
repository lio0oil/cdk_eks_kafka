import base64
import zlib
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws

from readings_pb2 import Reading, ReadingsMessage

BUCKET = "staging-test"
REGION = "ap-northeast-1"

SENT_AT = 1_788_000_000_123_456
MEASURED_AT = 1_787_999_999_000_001


@pytest.fixture(autouse=True)
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")


@pytest.fixture
def s3() -> Iterator[Any]:
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
        yield client


def make_message(
    device_id: str = "dev-1",
    device_code: str = "code-a",
    message_id: str = "msg-1",
    sent_at: int = SENT_AT,
    readings: list[tuple[str, float, int]] | None = None,
) -> ReadingsMessage:
    if readings is None:
        readings = [("temperature", 21.5, MEASURED_AT), ("humidity", 40.25, MEASURED_AT + 1_000_000)]
    return ReadingsMessage(
        device_id=device_id,
        device_code=device_code,
        message_id=message_id,
        sent_at=sent_at,
        readings=[Reading(metric=m, value=v, measured_at=t) for m, v, t in readings],
    )


def kafka_value(message: ReadingsMessage) -> bytes:
    return zlib.compress(message.SerializeToString())


def kafka_record(value: bytes, offset: int) -> dict[str, Any]:
    return {"value": base64.b64encode(value).decode("ascii"), "offset": offset}


def make_event(records: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {"eventSource": "SelfManagedKafka", "records": records}
