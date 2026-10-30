import argparse
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.client import BaseClient
from pyspark.sql import SparkSession

from incoming import DEFAULT_MARGIN_SECONDS, compute_cutoff, list_incoming, read_watermark, write_watermark
from merge import merge_files, new_run_id
from staging_schema import CONTRACT_PATH


def run(s3: BaseClient, bucket: str, now: datetime, margin_seconds: int, merge: Callable[[list[str]], None]) -> None:
    cutoff = compute_cutoff(now, margin_seconds)
    watermark = read_watermark(s3, bucket)
    files = list_incoming(s3, bucket, watermark, cutoff)
    if files:
        merge(files)
    # With nothing to process the watermark still advances: those minutes are closed, and a stale
    # watermark would make the next run list the same empty range again.
    last_closed_minute = cutoff - timedelta(minutes=1)
    write_watermark(s3, bucket, last_closed_minute if watermark is None else max(watermark, last_closed_minute))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-bucket", required=True)
    parser.add_argument("--target", required=True, help="Fully qualified Iceberg table, e.g. catalog.namespace.table")
    parser.add_argument("--margin-seconds", type=int, default=DEFAULT_MARGIN_SECONDS)
    parser.add_argument("--contract", default=str(CONTRACT_PATH), help="Path to staging_schema.json")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    spark = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    run_id = new_run_id(now)

    def merge(paths: list[str]) -> None:
        merge_files(spark, args.target, paths, run_id, now, contract_path=Path(args.contract))

    run(boto3.client("s3"), args.staging_bucket, now, args.margin_seconds, merge)


if __name__ == "__main__":
    main()
