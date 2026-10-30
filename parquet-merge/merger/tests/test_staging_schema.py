import json
from pathlib import Path

from pyspark.sql import SparkSession

from staging_schema import CONTRACT_PATH, SPARK_TYPES, staging_schema

ADDED_BY_MERGE = {"merge_run_id", "merged_at"}


def contract_columns() -> list[dict[str, str]]:
    return json.loads(Path(CONTRACT_PATH).read_text())["columns"]


def test_every_contract_type_is_in_the_conversion_table():
    assert {c["type"] for c in contract_columns()} <= set(SPARK_TYPES)


def test_staging_schema_matches_table_columns_except_those_added_by_the_merge(spark: SparkSession, table: str):
    table_fields = [(f.name, f.dataType) for f in spark.table(table).schema.fields if f.name not in ADDED_BY_MERGE]

    assert [(f.name, f.dataType) for f in staging_schema().fields] == table_fields
