from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from staging_schema import staging_schema

WRITTEN_AT = datetime(2026, 10, 30, 12, 0, 0, tzinfo=timezone.utc)


def utc(hour: int, minute: int = 0, day: int = 30) -> datetime:
    return datetime(2026, 10, day, hour, minute, 0, tzinfo=timezone.utc)


def reading(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "device_id": "dev-1",
        "message_id": "msg-1",
        "device_code": "code-a",
        "sent_at": utc(12),
        "measured_at": utc(11, 30),
        "metric": "temperature",
        "value": 1.0,
        "written_at": WRITTEN_AT,
    }
    row.update(overrides)
    return row


def write_staged(spark: SparkSession, directory: Path, rows: list[dict[str, Any]]) -> str:
    """Writes rows as a single Parquet file and returns its path."""
    schema = staging_schema()
    spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema).coalesce(1).write.parquet(
        str(directory)
    )
    (path,) = directory.glob("part-*.parquet")
    return str(path)
