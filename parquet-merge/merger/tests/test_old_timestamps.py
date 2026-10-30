from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from merge import merge_files
from tests.staged import MERGED_AT

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
WRITTEN_AT = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)

# Includes the two writer limits and dates before the Gregorian cutover (1582), where Spark's legacy
# Julian calendar handling differs from pyarrow's proleptic Gregorian one.
EXTREME_INSTANTS = [
    datetime(1, 1, 1, tzinfo=timezone.utc),
    datetime(1500, 6, 15, 12, 30, tzinfo=timezone.utc),
    datetime(1800, 3, 1, 0, 0, 1, tzinfo=timezone.utc),
    datetime(1899, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
]


def micros(instant: datetime) -> int:
    delta = instant - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def write_with_pyarrow(path: Path) -> str:
    """Writes the staging file the way the writer does, so Spark does not know it is a Spark file."""
    count = len(EXTREME_INSTANTS)
    timestamp = pa.timestamp("us", tz="UTC")
    table = pa.table(
        {
            "device_id": ["dev-1"] * count,
            "message_id": [f"msg-{i}" for i in range(count)],
            "device_code": ["code-a"] * count,
            "sent_at": pa.array([micros(i) for i in EXTREME_INSTANTS], timestamp),
            "measured_at": pa.array([micros(i) for i in EXTREME_INSTANTS], timestamp),
            "metric": ["temperature"] * count,
            "value": [1.0] * count,
            "written_at": pa.array([micros(WRITTEN_AT)] * count, timestamp),
        }
    )
    pq.write_table(table, path, compression="zstd")
    return str(path)


def test_pyarrow_written_extreme_timestamps_are_merged_and_read_back_unchanged(
    spark: SparkSession, table: str, tmp_path: Path
):
    path = write_with_pyarrow(tmp_path / "staged.parquet")

    merge_files(spark, table, [path], "run-1", MERGED_AT)

    stored = (
        spark.table(table)
        .select(
            "message_id", F.unix_micros("measured_at").alias("measured_at"), F.unix_micros("sent_at").alias("sent_at")
        )
        .collect()
    )
    assert sorted((r["message_id"], r["measured_at"], r["sent_at"]) for r in stored) == [
        (f"msg-{i}", micros(instant), micros(instant)) for i, instant in enumerate(EXTREME_INSTANTS)
    ]


def test_resending_extreme_timestamps_is_matched_against_the_right_hour_range(
    spark: SparkSession, table: str, tmp_path: Path
):
    path = write_with_pyarrow(tmp_path / "staged.parquet")
    merge_files(spark, table, [path], "run-1", MERGED_AT)

    merge_files(spark, table, [path], "run-2", MERGED_AT)

    assert spark.table(table).count() == len(EXTREME_INSTANTS)
