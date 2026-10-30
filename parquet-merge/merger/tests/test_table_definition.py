import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType, DoubleType, StringType, TimestampType

from staging_schema import CONTRACT_PATH

TABLE_DEFINITION_PATH = Path(__file__).resolve().parents[2] / "tables" / "sample_readings.json"

# Spark's TimestampType is the UTC-adjusted Iceberg timestamptz; plain "timestamp" would be a different type.
ICEBERG_TYPES = {"string": "string", "double": "double", "timestamp_us_utc": "timestamptz"}
SPARK_TYPES: dict[str, DataType] = {"string": StringType(), "double": DoubleType(), "timestamptz": TimestampType()}
ADDED_AFTER_STAGING = [("merge_run_id", "string"), ("merged_at", "timestamptz")]


@pytest.fixture(scope="module")
def iceberg() -> dict[str, Any]:
    return json.loads(TABLE_DEFINITION_PATH.read_text())["iceberg"]


def schema_fields(iceberg: dict[str, Any]) -> list[dict[str, Any]]:
    return iceberg["schema"]["fields"]


def field_id(iceberg: dict[str, Any], name: str) -> int:
    (found,) = [f["id"] for f in schema_fields(iceberg) if f["name"] == name]
    return found


def test_definition_is_a_metadata_document_for_s3_tables_create_table(iceberg: dict[str, Any]):
    assert set(json.loads(TABLE_DEFINITION_PATH.read_text())) == {"iceberg"}
    assert set(iceberg) <= {"schema", "schemaV2", "partitionSpec", "writeOrder", "properties"}


def test_columns_are_the_contract_columns_followed_by_those_added_by_the_merge(iceberg: dict[str, Any]):
    contract = json.loads(CONTRACT_PATH.read_text())["columns"]
    expected = [(c["name"], ICEBERG_TYPES[c["type"]]) for c in contract] + ADDED_AFTER_STAGING

    assert [(f["name"], f["type"]) for f in schema_fields(iceberg)] == expected


def test_column_ids_are_unique_positive_integers(iceberg: dict[str, Any]):
    ids = [f["id"] for f in schema_fields(iceberg)]

    assert len(set(ids)) == len(ids)
    assert all(isinstance(i, int) and i > 0 for i in ids)


def test_partitions_by_device_code_identity_and_measured_at_hour(iceberg: dict[str, Any]):
    fields = iceberg["partitionSpec"]["fields"]

    assert [(f["sourceId"], f["transform"]) for f in fields] == [
        (field_id(iceberg, "device_code"), "identity"),
        (field_id(iceberg, "measured_at"), "hour"),
    ]


def test_partition_field_ids_are_unique_and_do_not_collide_with_column_ids(iceberg: dict[str, Any]):
    ids = [f["fieldId"] for f in iceberg["partitionSpec"]["fields"]]

    assert len(set(ids)) == len(ids)
    assert all(i >= 1000 for i in ids)


def test_format_version_is_pinned_to_2_as_a_string(iceberg: dict[str, Any]):
    assert iceberg["properties"]["format-version"] == "2"


def test_sort_order_is_not_specified(iceberg: dict[str, Any]):
    assert "writeOrder" not in iceberg


def test_columns_and_partition_match_the_table_the_merge_tests_run_against(
    iceberg: dict[str, Any], spark: SparkSession, table: str
):
    schema = spark.table(table).schema
    partition_fields = spark.sql(f"SELECT partition FROM {table}.partitions").schema["partition"].dataType.fieldNames()  # pyright: ignore[reportAttributeAccessIssue]

    assert [(f["name"], SPARK_TYPES[f["type"]]) for f in schema_fields(iceberg)] == [
        (f.name, f.dataType) for f in schema.fields
    ]
    assert [f["name"] for f in iceberg["partitionSpec"]["fields"]] == partition_fields
