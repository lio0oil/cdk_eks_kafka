import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from staging_schema import CONTRACT_PATH, staging_schema

SOURCE_VIEW = "merge_source"
SOURCE_FILE = "_source_file"


def new_run_id(now: datetime) -> str:
    return f"{now.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


MICROS_PER_HOUR = 3_600_000_000


def _hour_start(hour_index: int) -> str:
    # Epoch microseconds rather than a datetime or a TIMESTAMP literal: the hour after 9999-12-31 23:00 is
    # year 10000, which neither Python's datetime nor strftime-built literals can express.
    return f"timestamp_micros({hour_index * MICROS_PER_HOUR})"


def match_condition(partitions: list[tuple[str, int]]) -> tuple[str, dict[str, str]]:
    """Builds the MERGE ON clause and its parameters from (device_code, hours since the epoch in UTC) pairs.

    device_code is device-supplied, so it is bound as a parameter rather than written into the SQL text. Spark
    expands ${...} anywhere in SQL text, including inside string literals, which would make a code like
    "${env:HOME}" match a different value than the one stored.
    """
    parameters = {f"code{i}": code for i, (code, _) in enumerate(partitions)}
    ranges = " OR ".join(
        f"(t.device_code = :code{i}"
        f" AND t.measured_at >= {_hour_start(hour)}"
        f" AND t.measured_at < {_hour_start(hour + 1)})"
        for i, (_, hour) in enumerate(partitions)
    )
    condition = (
        f"t.device_code = s.device_code AND t.device_id = s.device_id AND t.message_id = s.message_id AND ({ranges})"
    )
    return condition, parameters


def read_deduplicated(spark: SparkSession, paths: list[str], contract_path: Path = CONTRACT_PATH) -> DataFrame:
    """Keeps, for each (device_id, message_id), only the rows of the copy with the smallest (written_at, file)."""
    # The writer is pyarrow, which uses the proleptic Gregorian calendar like Spark 3 and stores timestamps as
    # INT64 microseconds. Left at the default (EXCEPTION), a single timestamp before 1900 would fail every run,
    # since the writer accepts years 1 to 9999.
    staged = (
        spark.read.schema(staging_schema(contract_path))
        .option("datetimeRebaseMode", "CORRECTED")
        .parquet(*paths)
        .withColumn(SOURCE_FILE, F.input_file_name())
    )
    key = Window.partitionBy("device_id", "message_id")
    chosen = F.min(F.struct("written_at", SOURCE_FILE)).over(key)
    return (
        staged.withColumn("_chosen", chosen).where(F.col(SOURCE_FILE) == F.col("_chosen")[SOURCE_FILE]).drop("_chosen")
    )


def _utc_hours(source: DataFrame) -> list[tuple[str, int]]:
    # Integer arithmetic keeps the hour boundaries in UTC regardless of the session time zone. Division by a
    # double would round near year 9999, where microseconds exceed 2^53, and pmod makes the division floor
    # for instants before the epoch.
    micros = "unix_micros(measured_at)"
    hour_index = F.expr(f"({micros} - pmod({micros}, {MICROS_PER_HOUR})) div {MICROS_PER_HOUR}").alias("hour_index")
    rows = source.select("device_code", hour_index).distinct().collect()
    return [(r["device_code"], r["hour_index"]) for r in rows]


def merge_files(
    spark: SparkSession,
    target: str,
    paths: list[str],
    merge_run_id: str,
    contract_path: Path = CONTRACT_PATH,
) -> None:
    source = read_deduplicated(spark, paths, contract_path).persist()
    try:
        condition, parameters = match_condition(sorted(_utc_hours(source)))
        columns = [f.name for f in staging_schema(contract_path).fields]
        source.select(*columns, F.lit(merge_run_id).alias("merge_run_id")).createOrReplaceTempView(SOURCE_VIEW)
        spark.sql(
            f"MERGE INTO {target} t USING {SOURCE_VIEW} s ON {condition} WHEN NOT MATCHED THEN INSERT *",
            args=parameters,
        )
    finally:
        source.unpersist()
