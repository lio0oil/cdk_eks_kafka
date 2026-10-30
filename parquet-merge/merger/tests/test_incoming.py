from datetime import datetime, timezone

import pytest
from botocore.client import BaseClient

from incoming import compute_cutoff, list_incoming, read_watermark, write_watermark
from tests.conftest import BUCKET


def at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 10, 30, hour, minute, second, tzinfo=timezone.utc)


def put(s3: BaseClient, key: str) -> None:
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")


def uris(*keys: str) -> list[str]:
    return [f"s3://{BUCKET}/{key}" for key in keys]


@pytest.mark.parametrize(
    ("now", "margin", "expected"),
    [
        (at(12, 7, 30), 120, at(12, 5)),
        (at(12, 7, 0), 120, at(12, 5)),
        (at(12, 7, 59), 120, at(12, 5)),
        (at(12, 8, 0), 120, at(12, 6)),
        (at(12, 1, 30), 120, at(11, 59)),
        (at(12, 7, 30), 0, at(12, 7)),
        (at(12, 7, 30), 300, at(12, 2)),
    ],
)
def test_cutoff_is_now_minus_margin_floored_to_the_minute(now: datetime, margin: int, expected: datetime):
    assert compute_cutoff(now, margin) == expected


def test_cutoff_margin_defaults_to_120_seconds():
    assert compute_cutoff(at(12, 7, 30)) == at(12, 5)


def test_watermark_is_none_when_the_state_object_does_not_exist(s3: BaseClient):
    assert read_watermark(s3, BUCKET) is None


def test_watermark_round_trips_through_state_watermark_json(s3: BaseClient):
    write_watermark(s3, BUCKET, at(12, 5))

    assert read_watermark(s3, BUCKET) == at(12, 5)
    assert s3.get_object(Bucket=BUCKET, Key="state/watermark.json")["Body"].read() == b'{"watermark": "20261030T1205"}'


def test_watermark_write_overwrites_the_previous_value(s3: BaseClient):
    write_watermark(s3, BUCKET, at(12, 5))
    write_watermark(s3, BUCKET, at(12, 9))

    assert read_watermark(s3, BUCKET) == at(12, 9)


def test_lists_only_minutes_after_the_watermark_and_before_the_cutoff(s3: BaseClient):
    for minute in ("1203", "1204", "1205", "1206", "1207"):
        put(s3, f"incoming/t=20261030T{minute}/batch.parquet")

    listed = list_incoming(s3, BUCKET, watermark=at(12, 4), cutoff=at(12, 7))

    assert listed == uris("incoming/t=20261030T1205/batch.parquet", "incoming/t=20261030T1206/batch.parquet")


def test_starts_from_the_oldest_minute_when_there_is_no_watermark(s3: BaseClient):
    for minute in ("1210", "1203", "1205"):
        put(s3, f"incoming/t=20261030T{minute}/batch.parquet")

    listed = list_incoming(s3, BUCKET, watermark=None, cutoff=at(12, 8))

    assert listed == uris("incoming/t=20261030T1203/batch.parquet", "incoming/t=20261030T1205/batch.parquet")


def test_the_cutoff_minute_and_later_minutes_are_not_listed(s3: BaseClient):
    put(s3, "incoming/t=20261030T1205/batch.parquet")
    put(s3, "incoming/t=20261030T1206/batch.parquet")
    put(s3, "incoming/t=20261030T1207/batch.parquet")

    assert list_incoming(s3, BUCKET, watermark=None, cutoff=at(12, 5)) == []


def test_lists_every_file_of_a_minute_in_key_order(s3: BaseClient):
    put(s3, "incoming/t=20261030T1205/b.parquet")
    put(s3, "incoming/t=20261030T1205/a.parquet")
    put(s3, "incoming/t=20261030T1206/c.parquet")

    listed = list_incoming(s3, BUCKET, watermark=None, cutoff=at(12, 7))

    assert listed == uris(
        "incoming/t=20261030T1205/a.parquet",
        "incoming/t=20261030T1205/b.parquet",
        "incoming/t=20261030T1206/c.parquet",
    )


def test_other_prefixes_are_not_listed(s3: BaseClient):
    put(s3, "dlq/t=20261030T1205/batch.parquet")
    put(s3, "state/watermark.json")
    put(s3, "incoming/t=20261030T1205/batch.parquet")

    assert list_incoming(s3, BUCKET, watermark=None, cutoff=at(12, 6)) == uris("incoming/t=20261030T1205/batch.parquet")


def test_listing_works_across_more_than_one_page_of_minutes(s3: BaseClient):
    for i in range(1100):
        total = 12 * 60 + i // 2
        put(s3, f"incoming/t=20261030T{total // 60 % 24:02d}{total % 60:02d}/b{i % 2}.parquet")
    for i in range(1100):
        put(s3, f"incoming/t=20261029T{i // 60 % 24:02d}{i % 60:02d}/x.parquet")

    listed = list_incoming(s3, BUCKET, watermark=at(12, 4), cutoff=at(12, 7))

    assert listed == uris(
        "incoming/t=20261030T1205/b0.parquet",
        "incoming/t=20261030T1205/b1.parquet",
        "incoming/t=20261030T1206/b0.parquet",
        "incoming/t=20261030T1206/b1.parquet",
    )
