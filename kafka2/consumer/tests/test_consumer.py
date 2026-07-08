import zlib
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, LongType, StringType, StructField, StructType
from pytest_mock import MockerFixture

import consumer

# kafka2/proto/event.proto の Envelope 構造を模した payload 列のスキーマ。
_HISTORY_RECORD_SCHEMA = StructType(
    [
        StructField("historyid", LongType()),
        StructField("datetime", StringType()),
        StructField("memo", StringType()),
        StructField("classification", StringType()),
    ]
)
_PAYLOAD_SCHEMA = StructType(
    [
        StructField("common", StructType([StructField("id", LongType()), StructField("datetime", StringType())])),
        StructField("user", StructType([StructField("userid", LongType()), StructField("username", StringType())])),
        StructField("content", StructType([StructField("historyrecords", ArrayType(_HISTORY_RECORD_SCHEMA))])),
    ]
)


def _make_envelope_row(classifications: list[str]) -> Row:
    return Row(
        common=Row(id=1, datetime="2026-07-08T00:00:00Z"),
        user=Row(userid=10, username="alice"),
        content=Row(
            historyrecords=[
                Row(historyid=100 + i, datetime="2026-07-08T00:00:00Z", memo=f"memo{i}", classification=c)
                for i, c in enumerate(classifications)
            ]
        ),
    )


class TestZlibDecompress:
    def test_returns_original_bytes(self) -> None:
        assert consumer._zlib_decompress(zlib.compress(b"hello")) == b"hello"

    def test_returns_none_for_corrupted_data(self) -> None:
        assert consumer._zlib_decompress(b"not-zlib-compressed") is None

    def test_returns_none_for_none_input(self) -> None:
        assert consumer._zlib_decompress(None) is None


class TestParseArgs:
    def test_defaults_to_default_starting_offsets(self) -> None:
        args = consumer.parse_args([])
        assert args.starting_offsets == consumer.DEFAULT_STARTING_OFFSETS

    def test_accepts_explicit_starting_offsets(self) -> None:
        args = consumer.parse_args(["--starting-offsets", "latest"])
        assert args.starting_offsets == "latest"

    def test_rejects_invalid_choice(self) -> None:
        with pytest.raises(SystemExit):
            consumer.parse_args(["--starting-offsets", "invalid"])


class TestWriteValid:
    def test_partitions_by_classification_and_keeps_single_historyrecord(
        self, spark: SparkSession, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """classification 毎に explode され、envelope 列の historyrecords が
        該当 classification の 1 件だけに絞られていることを検証する。
        """
        output_path = str(tmp_path / "output")
        mocker.patch.object(consumer, "OUTPUT_PATH", output_path)

        payload = _make_envelope_row(["classification-a", "classification-b"])
        valid = spark.createDataFrame(
            [Row(payload=payload)], schema=StructType([StructField("payload", _PAYLOAD_SCHEMA)])
        )

        consumer._write_valid(valid)

        top_level_partition_keys = {p.name.split("=")[0] for p in Path(output_path).glob("*=*")}
        assert top_level_partition_keys == {"classification"}

        written = spark.read.parquet(output_path)
        rows = written.select("classification", "envelope.content.historyrecords").collect()
        assert len(rows) == 2
        for row in rows:
            historyrecords = row["historyrecords"]
            assert len(historyrecords) == 1
            assert historyrecords[0]["classification"] == row["classification"]


class TestWriteDlq:
    def test_does_not_partition_by_reason(self, spark: SparkSession, tmp_path: Path, mocker: MockerFixture) -> None:
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


class TestWriteBatch:
    _BATCH_SCHEMA = StructType(
        [
            StructField("rawdata", BinaryType()),
            StructField("decompressed", BinaryType()),
            StructField("payload", _PAYLOAD_SCHEMA),
        ]
    )

    def test_routes_rows_by_decompressed_and_payload_nullness(
        self, spark: SparkSession, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """decompressed IS NULL は zlib_decompress_error、payload IS NULL は
        deserialize_error として DLQ へ、それ以外は OUTPUT_PATH へ書かれることを検証する。
        """
        output_path = str(tmp_path / "output")
        dlq_output_path = str(tmp_path / "dlq")
        mocker.patch.object(consumer, "OUTPUT_PATH", output_path)
        mocker.patch.object(consumer, "DLQ_OUTPUT_PATH", dlq_output_path)

        valid_payload = _make_envelope_row(["classification-a"])
        batch_df = spark.createDataFrame(
            [
                Row(rawdata=b"zlib-broken", decompressed=None, payload=None),
                Row(rawdata=b"proto-broken", decompressed=b"decompressed-bytes", payload=None),
                Row(rawdata=b"ok", decompressed=b"decompressed-bytes", payload=valid_payload),
            ],
            schema=self._BATCH_SCHEMA,
        )

        consumer._write_batch(batch_df, batch_id=1)

        dlq_reasons = {row["reason"] for row in spark.read.parquet(dlq_output_path).select("reason").collect()}
        assert dlq_reasons == {consumer.DLQ_REASON_ZLIB_ERROR, consumer.DLQ_REASON_DESERIALIZE_ERROR}

        assert spark.read.parquet(output_path).count() == 1

    def test_does_not_write_dlq_when_all_rows_are_valid(
        self, spark: SparkSession, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        output_path = str(tmp_path / "output")
        dlq_output_path = str(tmp_path / "dlq")
        mocker.patch.object(consumer, "OUTPUT_PATH", output_path)
        mocker.patch.object(consumer, "DLQ_OUTPUT_PATH", dlq_output_path)

        valid_payload = _make_envelope_row(["classification-a"])
        batch_df = spark.createDataFrame(
            [Row(rawdata=b"ok", decompressed=b"decompressed-bytes", payload=valid_payload)],
            schema=self._BATCH_SCHEMA,
        )

        consumer._write_batch(batch_df, batch_id=1)

        assert spark.read.parquet(output_path).count() == 1
        assert not Path(dlq_output_path).exists()
