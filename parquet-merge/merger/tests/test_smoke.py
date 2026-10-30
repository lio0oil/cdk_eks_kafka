import subprocess
import sys
from pathlib import Path

from pyspark.sql import Row, SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_table_is_format_version_2(spark: SparkSession, table: str) -> None:
    props = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}

    assert props["format-version"] == "2"


def test_table_partition_spec(spark: SparkSession, table: str) -> None:
    fields = spark.sql(f"SELECT partition FROM {table}.partitions").schema["partition"].dataType.fieldNames()  # pyright: ignore[reportAttributeAccessIssue]

    assert fields == ["device_code", "measured_at_hour"]


def test_insert_and_read_back(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"""
        INSERT INTO {table} VALUES (
            'dev-1', 'msg-1', 'code-a',
            TIMESTAMP '2026-10-30 12:00:00', TIMESTAMP '2026-10-30 11:59:30',
            'temperature', 21.5,
            TIMESTAMP '2026-10-30 12:01:00', 'run-1',
            TIMESTAMP '2026-10-30 12:02:00'
        )
        """
    )

    rows = spark.table(table).collect()

    assert rows == [
        Row(
            device_id="dev-1",
            message_id="msg-1",
            device_code="code-a",
            sent_at=rows[0]["sent_at"],
            measured_at=rows[0]["measured_at"],
            metric="temperature",
            value=21.5,
            written_at=rows[0]["written_at"],
            merge_run_id="run-1",
            merged_at=rows[0]["merged_at"],
        )
    ]


def test_tables_are_independent_per_test(spark: SparkSession, table: str) -> None:
    assert spark.table(table).count() == 0


def test_unittest_mock_is_banned_by_ruff() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--no-cache",
            "--config",
            str(PROJECT_ROOT / "pyproject.toml"),
            "--stdin-filename",
            "snippet.py",
            "-",
        ],
        input="from unittest import mock\nfrom unittest.mock import MagicMock\n\n__all__ = [mock, MagicMock]\n",
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert result.stdout.count("TID251") == 2
