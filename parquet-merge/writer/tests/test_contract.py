import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contract" / "staging_schema.json"

EXPECTED_COLUMNS = [
    ("device_id", "string"),
    ("message_id", "string"),
    ("device_code", "string"),
    ("sent_at", "timestamp_us_utc"),
    ("measured_at", "timestamp_us_utc"),
    ("metric", "string"),
    ("value", "double"),
    ("written_at", "timestamp_us_utc"),
]

ARROW_TYPES = {
    "string": pa.string(),
    "double": pa.float64(),
    "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
}


def load_columns() -> list[dict[str, str]]:
    return json.loads(CONTRACT_PATH.read_text())["columns"]


def test_contract_columns_and_types():
    assert [(c["name"], c["type"]) for c in load_columns()] == EXPECTED_COLUMNS


def test_contract_does_not_contain_merge_run_id():
    assert "merge_run_id" not in [c["name"] for c in load_columns()]


def test_contract_types_use_known_vocabulary():
    assert {c["type"] for c in load_columns()} <= set(ARROW_TYPES)


def test_contract_converts_to_arrow_schema_that_survives_parquet(tmp_path: Path):
    schema = pa.schema([(c["name"], ARROW_TYPES[c["type"]]) for c in load_columns()])
    path = tmp_path / "staging.parquet"
    pq.write_table(schema.empty_table(), path, compression="zstd")

    assert pq.read_schema(path).equals(schema)
