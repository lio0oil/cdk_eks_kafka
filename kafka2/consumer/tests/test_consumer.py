from pathlib import Path

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pytest_mock import MockerFixture

import consumer


def test_write_dlq_does_not_partition_by_reason(spark: SparkSession, tmp_path: Path, mocker: MockerFixture) -> None:
    """DLQ の reason は分析対象ではないため partition column に含めない。

    reason 別のディレクトリ (reason=xxx/) を作らず、year/month/day のみで
    partition することを検証する。reason 列自体はデータとして残す。
    """
    dlq_output_path = str(tmp_path / "dlq")
    mocker.patch.object(consumer, "DLQ_OUTPUT_PATH", dlq_output_path)

    invalid = spark.createDataFrame(
        [
            Row(failed_at="2026-07-08 00:00:00", rawdata=b"broken-zlib", reason="zlib_decompress_error"),
            Row(failed_at="2026-07-08 00:00:00", rawdata=b"broken-proto", reason="deserialize_error"),
        ]
    ).withColumn("failed_at", F.col("failed_at").cast("timestamp"))

    consumer._write_dlq(invalid)

    top_level_partition_keys = {p.name.split("=")[0] for p in Path(dlq_output_path).glob("*=*")}
    assert top_level_partition_keys == {"year"}

    written = spark.read.parquet(dlq_output_path)
    reasons = {row["reason"] for row in written.select("reason").distinct().collect()}
    assert reasons == {"zlib_decompress_error", "deserialize_error"}
