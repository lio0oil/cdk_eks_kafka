import json
from pathlib import Path

from pyspark.sql import SparkSession

from staging_schema import CONTRACT_PATH, SPARK_TYPES, staging_schema


def contract_columns() -> list[dict[str, str]]:
    return json.loads(Path(CONTRACT_PATH).read_text())["columns"]


def test_every_contract_type_is_in_the_conversion_table():
    assert {c["type"] for c in contract_columns()} <= set(SPARK_TYPES)


def test_staging_schema_matches_table_columns_except_merge_run_id(spark: SparkSession, table: str):
    table_fields = [(f.name, f.dataType) for f in spark.table(table).schema.fields if f.name != "merge_run_id"]

    assert [(f.name, f.dataType) for f in staging_schema().fields] == table_fields
