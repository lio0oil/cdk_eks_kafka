import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import pytest
from botocore.client import BaseClient
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import job
from incoming import read_watermark, write_watermark
from job import main, parse_args, run
from merge import merge_files
from tests.conftest import BUCKET
from tests.staged import MERGED_AT, reading, utc, write_staged


def at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 10, 30, hour, minute, second, tzinfo=timezone.utc)


def upload(s3: BaseClient, minute: str, name: str, parquet_path: str) -> str:
    key = f"incoming/t=20261030T{minute}/{name}.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=Path(parquet_path).read_bytes())
    return f"s3://{BUCKET}/{key}"


def merging_into(
    spark: SparkSession, table: str, s3: BaseClient, workdir: Path, run_id: str = "run"
) -> Callable[[list[str]], None]:
    """Spark reads the local filesystem, so the listed objects are fetched from S3 first."""

    def merge(uris: list[str]) -> None:
        local = []
        for i, uri in enumerate(uris):
            key = uri.removeprefix(f"s3://{BUCKET}/")
            path = workdir / f"fetched-{i}-{key.replace('/', '_')}"
            path.write_bytes(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            local.append(str(path))
        merge_files(spark, table, local, run_id, MERGED_AT)

    return merge


def test_run_merges_listed_minutes_and_advances_the_watermark(
    spark: SparkSession, table: str, s3: BaseClient, tmp_path: Path
):
    for minute, message_id in (("1203", "m1"), ("1204", "m2"), ("1205", "m3"), ("1210", "m4")):
        staged = write_staged(spark, tmp_path / minute, [reading(message_id=message_id)])
        upload(s3, minute, "batch", staged)

    run(s3, BUCKET, at(12, 7, 30), 120, merging_into(spark, table, s3, tmp_path))

    assert {r["message_id"] for r in spark.table(table).collect()} == {"m1", "m2"}
    assert read_watermark(s3, BUCKET) == at(12, 4)


def test_second_run_only_merges_minutes_after_the_watermark(mocker: MockerFixture, s3: BaseClient):
    for minute in ("1203", "1204", "1205", "1206"):
        s3.put_object(Bucket=BUCKET, Key=f"incoming/t=20261030T{minute}/batch.parquet", Body=b"x")
    merge = mocker.Mock()

    run(s3, BUCKET, at(12, 7, 30), 120, merge)
    run(s3, BUCKET, at(12, 9, 30), 120, merge)

    assert [c.args[0] for c in merge.call_args_list] == [
        [
            f"s3://{BUCKET}/incoming/t=20261030T1203/batch.parquet",
            f"s3://{BUCKET}/incoming/t=20261030T1204/batch.parquet",
        ],
        [
            f"s3://{BUCKET}/incoming/t=20261030T1205/batch.parquet",
            f"s3://{BUCKET}/incoming/t=20261030T1206/batch.parquet",
        ],
    ]
    assert read_watermark(s3, BUCKET) == at(12, 6)


def test_nothing_to_process_skips_the_merge_and_moves_the_watermark_to_the_last_closed_minute(
    mocker: MockerFixture, s3: BaseClient
):
    write_watermark(s3, BUCKET, at(12, 0))
    merge = mocker.Mock()

    run(s3, BUCKET, at(12, 6, 30), 120, merge)

    merge.assert_not_called()
    assert read_watermark(s3, BUCKET) == at(12, 3)


def test_the_watermark_never_moves_backwards(mocker: MockerFixture, s3: BaseClient):
    write_watermark(s3, BUCKET, at(12, 10))

    run(s3, BUCKET, at(12, 6, 30), 120, mocker.Mock())

    assert read_watermark(s3, BUCKET) == at(12, 10)


def test_a_file_put_into_the_cutoff_minute_after_a_run_is_merged_by_the_next_run(
    spark: SparkSession, table: str, s3: BaseClient, tmp_path: Path
):
    merge = merging_into(spark, table, s3, tmp_path)
    upload(s3, "1204", "early", write_staged(spark, tmp_path / "a", [reading(message_id="m1")]))
    run(s3, BUCKET, at(12, 7, 30), 120, merge)
    # The writer labels a file with the minute its invocation started, but the PUT can come later,
    # so the cutoff minute may still gain files after this run.
    upload(s3, "1205", "late", write_staged(spark, tmp_path / "b", [reading(message_id="m2")]))

    run(s3, BUCKET, at(12, 9, 30), 120, merge)

    assert {r["message_id"] for r in spark.table(table).collect()} == {"m1", "m2"}


def test_failed_merge_leaves_the_watermark_untouched(mocker: MockerFixture, s3: BaseClient):
    write_watermark(s3, BUCKET, at(12, 0))
    s3.put_object(Bucket=BUCKET, Key="incoming/t=20261030T1203/batch.parquet", Body=b"x")
    merge = mocker.Mock(side_effect=RuntimeError("commit failed"))

    with pytest.raises(RuntimeError, match="commit failed"):
        run(s3, BUCKET, at(12, 6, 30), 120, merge)

    assert read_watermark(s3, BUCKET) == at(12, 0)


def test_rerun_after_a_crash_between_commit_and_watermark_update_adds_no_duplicates(
    spark: SparkSession, table: str, s3: BaseClient, tmp_path: Path, mocker: MockerFixture
):
    spanning = [
        reading(metric="temperature", measured_at=utc(11, 59)),
        reading(metric="humidity", measured_at=utc(12, 1)),
    ]
    upload(s3, "1203", "batch", write_staged(spark, tmp_path / "a", spanning))
    upload(
        s3, "1204", "resent", write_staged(spark, tmp_path / "b", [{**r, "written_at": utc(12, 4)} for r in spanning])
    )
    merge = merging_into(spark, table, s3, tmp_path)
    mocker.patch.object(job, "write_watermark", side_effect=RuntimeError("crashed"))
    with pytest.raises(RuntimeError, match="crashed"):
        run(s3, BUCKET, at(12, 7, 30), 120, merge)
    assert spark.table(table).count() == 2
    assert read_watermark(s3, BUCKET) is None
    mocker.stopall()

    run(s3, BUCKET, at(12, 7, 40), 120, merge)

    assert spark.table(table).count() == 2
    assert read_watermark(s3, BUCKET) == at(12, 4)


def test_watermark_object_is_a_small_json_document(mocker: MockerFixture, s3: BaseClient):
    run(s3, BUCKET, at(12, 6, 30), 120, mocker.Mock())

    body = s3.get_object(Bucket=BUCKET, Key="state/watermark.json")["Body"].read()
    assert json.loads(body) == {"watermark": "20261030T1203"}


def test_arguments_default_the_margin_to_120_seconds():
    args = parse_args(["--staging-bucket", "b", "--target", "cat.db.t"])

    assert (args.staging_bucket, args.target, args.margin_seconds) == ("b", "cat.db.t", 120)


def test_arguments_accept_a_margin():
    args = parse_args(["--staging-bucket", "b", "--target", "cat.db.t", "--margin-seconds", "300"])

    assert args.margin_seconds == 300


def test_main_uses_one_instant_for_both_the_run_id_and_merged_at(mocker: MockerFixture):
    mocker.patch.object(job, "boto3")
    mocker.patch.object(job, "SparkSession")
    run_mock = mocker.patch.object(job, "run")
    merge_files_mock = mocker.patch.object(job, "merge_files")

    main(["--staging-bucket", "b", "--target", "cat.db.t"])
    _, _, now, _, merge = run_mock.call_args.args
    merge(["s3://b/incoming/x.parquet"])

    (_, _, _, run_id, merged_at), _ = merge_files_mock.call_args
    assert merged_at == now
    assert run_id.startswith(f"{now:%Y%m%dT%H%M%SZ}-")
