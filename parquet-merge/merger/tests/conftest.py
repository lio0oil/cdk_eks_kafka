import itertools
from collections.abc import Generator
from pathlib import Path

import boto3
import pytest
from botocore.client import BaseClient
from moto import mock_aws
from pyspark.sql import SparkSession

CATALOG = "local"
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"

_table_counter = itertools.count()


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")  # pyright: ignore[reportAttributeAccessIssue]
        .appName("merger-test")
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(Path(warehouse)))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.db")
    yield session
    session.stop()


@pytest.fixture
def table(spark: SparkSession) -> Generator[str, None, None]:
    name = f"{CATALOG}.db.sample_readings_{next(_table_counter)}"
    spark.sql(
        f"""
        CREATE TABLE {name} (
            device_id string,
            message_id string,
            device_code string,
            sent_at timestamp,
            measured_at timestamp,
            metric string,
            value double,
            written_at timestamp,
            merge_run_id string,
            merged_at timestamp
        )
        USING iceberg
        PARTITIONED BY (device_code, hour(measured_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")


BUCKET = "staging"


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> Generator[BaseClient, None, None]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client
