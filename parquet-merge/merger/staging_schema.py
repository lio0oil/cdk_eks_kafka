import json
from pathlib import Path

from pyspark.sql.types import DataType, DoubleType, StringType, StructField, StructType, TimestampType

CONTRACT_PATH = Path(__file__).resolve().parent.parent / "contract" / "staging_schema.json"

SPARK_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "double": DoubleType(),
    "timestamp_us_utc": TimestampType(),
}


def staging_schema(contract_path: Path = CONTRACT_PATH) -> StructType:
    columns = json.loads(contract_path.read_text())["columns"]
    return StructType([StructField(c["name"], SPARK_TYPES[c["type"]]) for c in columns])
