import importlib
import json
import logging
import zlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from conftest import BUCKET, MEASURED_AT, SENT_AT, kafka_record, kafka_value, make_event, make_message
from pytest_mock import MockerFixture

import handler
from schema import load_schema

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contract" / "staging_schema.json"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NOW = datetime(2026, 10, 30, 12, 5, 42, 123_456, tzinfo=UTC)
MAX_US = 253_402_300_799_999_999
MIN_US = -62_135_596_800_000_000


def run(s3: Any, event: dict[str, Any], now: datetime = NOW, max_decompressed_bytes: int = 1_000_000) -> None:
    handler.handle(
        event,
        s3=s3,
        bucket=BUCKET,
        schema=load_schema(CONTRACT_PATH),
        now=now,
        max_decompressed_bytes=max_decompressed_bytes,
    )


def keys(s3: Any, prefix: str) -> list[str]:
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o["Key"] for o in response.get("Contents", [])]


def read_table(s3: Any, key: str) -> pa.Table:
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pq.read_table(pa.BufferReader(body))


def only_table(s3: Any, prefix: str) -> pa.Table:
    (key,) = keys(s3, prefix)
    return read_table(s3, key)


def at(micros: int) -> datetime:
    return EPOCH + timedelta(microseconds=micros)


def test_output_schema_matches_contract(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}))

    contract = json.loads(CONTRACT_PATH.read_text())["columns"]
    arrow_types = {
        "string": pa.string(),
        "double": pa.float64(),
        "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
    }
    expected = pa.schema([(c["name"], arrow_types[c["type"]]) for c in contract])
    assert only_table(s3, "incoming/").schema.equals(expected)


def test_one_message_becomes_one_row_per_reading(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}))

    rows = only_table(s3, "incoming/").to_pylist()
    assert len(rows) == 2
    assert {r["written_at"] for r in rows} == {NOW}
    assert {r["device_code"] for r in rows} == {"code-a"}
    assert {(r["device_id"], r["message_id"]) for r in rows} == {("dev-1", "msg-1")}
    assert {r["sent_at"] for r in rows} == {at(SENT_AT)}
    assert [(r["metric"], r["value"], r["measured_at"]) for r in rows] == [
        ("temperature", 21.5, at(MEASURED_AT)),
        ("humidity", 40.25, at(MEASURED_AT + 1_000_000)),
    ]


def test_unix_microseconds_are_stored_without_rounding(s3: Any):
    message = make_message(sent_at=1_788_000_000_123_457, readings=[("m", 1.0, 1_787_999_999_000_003)])
    run(s3, make_event({"t-0": [kafka_record(kafka_value(message), 0)]}))

    table = only_table(s3, "incoming/")
    assert table["sent_at"].cast(pa.int64()).to_pylist() == [1_788_000_000_123_457]
    assert table["measured_at"].cast(pa.int64()).to_pylist() == [1_787_999_999_000_003]
    assert table["written_at"].cast(pa.int64()).to_pylist() == [int((NOW - EPOCH) / timedelta(microseconds=1))]


def test_parquet_is_zstd_compressed(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}))

    (key,) = keys(s3, "incoming/")
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert pq.ParquetFile(pa.BufferReader(body)).metadata.row_group(0).column(0).compression == "ZSTD"


def test_records_of_several_partitions_go_to_one_file(s3: Any):
    event = make_event(
        {
            "my-topic-0": [kafka_record(kafka_value(make_message(message_id="a")), 5)],
            "my-topic-1": [kafka_record(kafka_value(make_message(message_id="b")), 9)],
        }
    )
    run(s3, event)

    assert len(keys(s3, "incoming/")) == 1
    assert {r["message_id"] for r in only_table(s3, "incoming/").to_pylist()} == {"a", "b"}


def test_object_key_uses_put_minute_in_utc(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}))

    (key,) = keys(s3, "incoming/")
    assert key.startswith("incoming/t=20261030T1205/")
    assert key.endswith(".parquet")


def test_minute_is_floored_not_rounded(s3: Any):
    now = datetime(2026, 10, 30, 23, 59, 59, 999_999, tzinfo=UTC)
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}), now=now)

    assert keys(s3, "incoming/")[0].startswith("incoming/t=20261030T2359/")


def test_batch_id_is_deterministic_and_depends_on_offsets(s3: Any):
    value = kafka_value(make_message())
    run(s3, make_event({"t-0": [kafka_record(value, 10), kafka_record(value, 11)]}))
    first = keys(s3, "incoming/")
    run(s3, make_event({"t-0": [kafka_record(value, 10), kafka_record(value, 11)]}))
    assert keys(s3, "incoming/") == first

    run(s3, make_event({"t-0": [kafka_record(value, 10), kafka_record(value, 12)]}))
    assert len(keys(s3, "incoming/")) == 2


def test_batch_id_depends_on_topic_and_partition(s3: Any):
    value = kafka_value(make_message())
    run(s3, make_event({"t-0": [kafka_record(value, 1)]}))
    run(s3, make_event({"t-1": [kafka_record(value, 1)]}))
    run(s3, make_event({"u-0": [kafka_record(value, 1)]}))

    assert len(keys(s3, "incoming/")) == 3


def test_batch_id_ignores_record_order_inside_a_partition(s3: Any):
    value = kafka_value(make_message())
    run(s3, make_event({"t-0": [kafka_record(value, 1), kafka_record(value, 2), kafka_record(value, 3)]}))
    run(s3, make_event({"t-0": [kafka_record(value, 3), kafka_record(value, 2), kafka_record(value, 1)]}))

    assert len(keys(s3, "incoming/")) == 1


def test_retry_of_same_event_overwrites_objects(s3: Any):
    event = make_event(
        {"t-0": [kafka_record(kafka_value(make_message()), 0), kafka_record(b"not zlib", 1)]},
    )
    run(s3, event)
    run(s3, event)

    assert len(keys(s3, "incoming/")) == 1
    assert len(keys(s3, "dlq/")) == 1
    assert len(only_table(s3, "incoming/")) == 2


def test_duplicate_message_inside_batch_keeps_first_only(s3: Any):
    first = make_message(readings=[("first", 1.0, MEASURED_AT)])
    second = make_message(readings=[("second", 2.0, MEASURED_AT), ("second", 3.0, MEASURED_AT)])
    run(s3, make_event({"t-0": [kafka_record(kafka_value(first), 0), kafka_record(kafka_value(second), 1)]}))

    assert [r["metric"] for r in only_table(s3, "incoming/").to_pylist()] == ["first"]


def test_same_message_id_from_other_device_is_not_a_duplicate(s3: Any):
    a = make_message(device_id="dev-1", readings=[("m", 1.0, MEASURED_AT)])
    b = make_message(device_id="dev-2", readings=[("m", 1.0, MEASURED_AT)])
    run(s3, make_event({"t-0": [kafka_record(kafka_value(a), 0), kafka_record(kafka_value(b), 1)]}))

    assert {r["device_id"] for r in only_table(s3, "incoming/").to_pylist()} == {"dev-1", "dev-2"}


def test_duplicate_across_partitions_keeps_first_only(s3: Any):
    message = make_message(readings=[("m", 1.0, MEASURED_AT)])
    value = kafka_value(message)
    run(s3, make_event({"t-0": [kafka_record(value, 0)], "t-1": [kafka_record(value, 0)]}))

    assert len(only_table(s3, "incoming/")) == 1


def test_batch_id_ignores_partition_order_in_the_event(s3: Any):
    value = kafka_value(make_message())
    run(s3, make_event({"t-0": [kafka_record(value, 1)], "t-1": [kafka_record(value, 5)]}))
    run(s3, make_event({"t-1": [kafka_record(value, 5)], "t-0": [kafka_record(value, 1)]}))

    assert len(keys(s3, "incoming/")) == 1


def corrupt_zlib() -> bytes:
    return b"this is not zlib data"


def truncated_zlib() -> bytes:
    return kafka_value(make_message())[:-6]


def corrupt_protobuf() -> bytes:
    return zlib.compress(b"\xff\xff\xff\xff\xff\xff")


def empty_device_code() -> bytes:
    return kafka_value(make_message(device_code=""))


def empty_device_id() -> bytes:
    return kafka_value(make_message(device_id=""))


def empty_message_id() -> bytes:
    return kafka_value(make_message(message_id=""))


def sent_at_too_large() -> bytes:
    return kafka_value(make_message(sent_at=MAX_US + 1))


def sent_at_too_small() -> bytes:
    return kafka_value(make_message(sent_at=MIN_US - 1))


def measured_at_out_of_range() -> bytes:
    return kafka_value(make_message(readings=[("ok", 1.0, MEASURED_AT), ("bad", 1.0, MAX_US + 1)]))


@pytest.mark.parametrize(
    ("bad_value", "reason"),
    [
        (corrupt_zlib, "zlib_error"),
        (truncated_zlib, "zlib_error"),
        (corrupt_protobuf, "decode_error"),
        (empty_device_code, "invalid_field"),
        (empty_device_id, "invalid_field"),
        (empty_message_id, "invalid_field"),
        (sent_at_too_large, "invalid_field"),
        (sent_at_too_small, "invalid_field"),
        (measured_at_out_of_range, "invalid_field"),
    ],
)
def test_broken_record_goes_to_dlq_and_others_are_written(s3: Any, bad_value: Any, reason: str):
    good = kafka_value(make_message(message_id="good"))
    bad = bad_value()
    event = make_event({"my-topic-3": [kafka_record(good, 40), kafka_record(bad, 41)]})

    run(s3, event)

    assert {r["message_id"] for r in only_table(s3, "incoming/").to_pylist()} == {"good"}
    dlq = only_table(s3, "dlq/")
    assert dlq.to_pylist() == [{"topic": "my-topic", "partition": 3, "offset": 41, "reason": reason, "payload": bad}]


def test_too_large_after_decompression_goes_to_dlq(s3: Any):
    big = make_message(device_id="x" * 5_000)
    event = make_event(
        {
            "t-0": [
                kafka_record(kafka_value(make_message(message_id="good")), 0),
                kafka_record(kafka_value(big), 1),
            ]
        }
    )

    run(s3, event, max_decompressed_bytes=1_000)

    assert {r["message_id"] for r in only_table(s3, "incoming/").to_pylist()} == {"good"}
    dlq = only_table(s3, "dlq/").to_pylist()
    assert [(r["offset"], r["reason"]) for r in dlq] == [(1, "too_large")]


def test_highly_compressed_payload_goes_to_dlq_as_too_large(s3: Any):
    bomb = zlib.compress(b"\0" * (64 * 1024 * 1024))
    run(s3, make_event({"t-0": [kafka_record(bomb, 0)]}), max_decompressed_bytes=1024)

    assert [(r["offset"], r["reason"]) for r in only_table(s3, "dlq/").to_pylist()] == [(0, "too_large")]
    assert keys(s3, "incoming/") == []


def test_records_without_identity_are_not_deduplicated_against_each_other(s3: Any):
    value = kafka_value(make_message(message_id=""))
    run(s3, make_event({"t-0": [kafka_record(value, 0), kafka_record(value, 1)]}))

    assert [(r["offset"], r["reason"]) for r in only_table(s3, "dlq/").to_pylist()] == [
        (0, "invalid_field"),
        (1, "invalid_field"),
    ]
    assert keys(s3, "incoming/") == []


def test_message_at_exactly_the_size_limit_is_accepted(s3: Any):
    message = make_message()
    size = len(message.SerializeToString())
    run(s3, make_event({"t-0": [kafka_record(kafka_value(message), 0)]}), max_decompressed_bytes=size)

    assert keys(s3, "dlq/") == []
    assert len(only_table(s3, "incoming/")) == 2


def test_timestamp_range_boundaries_are_accepted(s3: Any):
    message = make_message(sent_at=MIN_US, readings=[("m", 1.0, MAX_US)])
    run(s3, make_event({"t-0": [kafka_record(kafka_value(message), 0)]}))

    assert keys(s3, "dlq/") == []
    row = only_table(s3, "incoming/").to_pylist()[0]
    assert row["sent_at"] == at(MIN_US)
    assert row["measured_at"] == at(MAX_US)


def test_several_broken_records_share_one_dlq_file(s3: Any):
    event = make_event({"t-0": [kafka_record(corrupt_zlib(), 0), kafka_record(corrupt_protobuf(), 1)]})
    run(s3, event)

    assert [r["reason"] for r in only_table(s3, "dlq/").to_pylist()] == ["zlib_error", "decode_error"]
    assert keys(s3, "dlq/")[0].startswith("dlq/t=20261030T1205/")


def test_invalid_message_does_not_hide_a_later_valid_copy(s3: Any):
    bad = kafka_value(make_message(message_id="same", sent_at=MAX_US + 1))
    good = kafka_value(make_message(message_id="same"))
    run(s3, make_event({"t-0": [kafka_record(bad, 0), kafka_record(good, 1)]}))

    assert len(only_table(s3, "incoming/")) == 2


def test_no_incoming_file_when_every_record_is_broken(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(corrupt_zlib(), 0)]}))

    assert keys(s3, "incoming/") == []
    assert len(keys(s3, "dlq/")) == 1


def test_no_dlq_file_when_every_record_is_valid(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}))

    assert keys(s3, "dlq/") == []


def test_no_incoming_file_when_messages_have_no_readings(s3: Any):
    run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message(readings=[])), 0)]}))

    assert keys(s3, "") == []


def test_incoming_put_failure_propagates(s3: Any, mocker: MockerFixture):
    mocker.patch.object(s3, "put_object", side_effect=ClientError({"Error": {"Code": "InternalError"}}, "PutObject"))

    with pytest.raises(ClientError):
        run(s3, make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}))


def test_dlq_put_failure_propagates(s3: Any, mocker: MockerFixture):
    real_put_object = s3.put_object

    def fail_for_dlq(**kwargs: Any) -> Any:
        if kwargs["Key"].startswith("dlq/"):
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        return real_put_object(**kwargs)

    mocker.patch.object(s3, "put_object", side_effect=fail_for_dlq)
    event = make_event({"t-0": [kafka_record(kafka_value(make_message()), 0), kafka_record(corrupt_zlib(), 1)]})

    with pytest.raises(ClientError):
        run(s3, event)


@pytest.fixture
def lambda_env(s3: Any, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture) -> Iterator[None]:
    monkeypatch.setenv("STAGING_BUCKET", BUCKET)
    mocker.patch.object(handler, "DEFAULT_SCHEMA_PATH", CONTRACT_PATH)
    mocker.patch.object(handler, "utcnow", return_value=NOW)
    handler.s3_client.cache_clear()
    yield
    handler.s3_client.cache_clear()


def test_lambda_handler_reads_bucket_from_environment_and_current_time(s3: Any, lambda_env: None):
    handler.lambda_handler(make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}), None)

    (key,) = keys(s3, "incoming/")
    assert key.startswith("incoming/t=20261030T1205/")
    assert {r["written_at"] for r in read_table(s3, key).to_pylist()} == {NOW}


def test_lambda_handler_ignores_schema_path_environment_variable(
    s3: Any, lambda_env: None, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("STAGING_SCHEMA_PATH", "/nonexistent/schema.json")

    handler.lambda_handler(make_event({"t-0": [kafka_record(kafka_value(make_message()), 0)]}), None)

    assert len(keys(s3, "incoming/")) == 1


def test_importing_handler_does_not_change_root_logger_level():
    root = logging.getLogger()
    before = root.level
    root.setLevel(logging.WARNING)
    try:
        importlib.reload(handler)
        assert root.level == logging.WARNING
    finally:
        root.setLevel(before)
