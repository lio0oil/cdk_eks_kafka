import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from merge import match_condition, merge_files, new_run_id
from tests.staged import reading, utc, write_staged


def table_rows(spark: SparkSession, table: str) -> list[tuple[str, str, float]]:
    rows = spark.table(table).collect()
    return sorted((r["message_id"], r["metric"], r["value"]) for r in rows)


def test_copies_in_different_files_keep_only_the_rows_of_one_file(spark: SparkSession, table: str, tmp_path: Path):
    original = write_staged(
        spark,
        tmp_path / "a",
        [reading(metric="temperature", value=1.0), reading(metric="humidity", value=2.0)],
    )
    resent = write_staged(
        spark,
        tmp_path / "b",
        [
            reading(metric="temperature", value=10.0, written_at=utc(12, 1)),
            reading(metric="humidity", value=20.0, written_at=utc(12, 1)),
            reading(metric="pressure", value=30.0, written_at=utc(12, 1)),
        ],
    )

    merge_files(spark, table, [resent, original], "run-1")

    assert table_rows(spark, table) == [("msg-1", "humidity", 2.0), ("msg-1", "temperature", 1.0)]


def test_copy_with_earliest_written_at_wins_even_if_its_path_sorts_later(
    spark: SparkSession, table: str, tmp_path: Path
):
    early = write_staged(spark, tmp_path / "z-early", [reading(value=1.0, written_at=utc(12, 0))])
    late = write_staged(spark, tmp_path / "a-late", [reading(value=2.0, written_at=utc(12, 5))])

    merge_files(spark, table, [late, early], "run-1")

    assert table_rows(spark, table) == [("msg-1", "temperature", 1.0)]


def test_ties_on_written_at_are_broken_by_source_file_path(spark: SparkSession, table: str, tmp_path: Path):
    second = write_staged(spark, tmp_path / "b", [reading(value=2.0)])
    first = write_staged(spark, tmp_path / "a", [reading(value=1.0)])

    merge_files(spark, table, [second, first], "run-1")

    assert table_rows(spark, table) == [("msg-1", "temperature", 1.0)]


def test_message_already_in_table_is_not_inserted(spark: SparkSession, table: str, tmp_path: Path):
    first = write_staged(spark, tmp_path / "a", [reading(message_id="msg-1", value=1.0)])
    merge_files(spark, table, [first], "run-1")
    resent = write_staged(
        spark,
        tmp_path / "b",
        [reading(message_id="msg-1", value=9.0, written_at=utc(12, 5)), reading(message_id="msg-2", value=2.0)],
    )

    merge_files(spark, table, [resent], "run-2")

    assert table_rows(spark, table) == [("msg-1", "temperature", 1.0), ("msg-2", "temperature", 2.0)]


def test_processing_the_same_files_twice_does_not_change_row_count(spark: SparkSession, table: str, tmp_path: Path):
    path = write_staged(
        spark,
        tmp_path / "a",
        [reading(message_id="msg-1"), reading(message_id="msg-1", metric="humidity"), reading(message_id="msg-2")],
    )

    merge_files(spark, table, [path], "run-1")
    merge_files(spark, table, [path], "run-2")

    assert spark.table(table).count() == 3
    assert {r["merge_run_id"] for r in spark.table(table).collect()} == {"run-1"}


def test_late_data_with_old_measured_at_is_matched_against_its_own_partition(
    spark: SparkSession, table: str, tmp_path: Path
):
    old = utc(3, 10, day=20)
    recent = utc(11, 30)
    first = write_staged(
        spark,
        tmp_path / "a",
        [reading(message_id="old-1", measured_at=old), reading(message_id="recent-1", measured_at=recent)],
    )
    merge_files(spark, table, [first], "run-1")
    late = write_staged(
        spark,
        tmp_path / "b",
        [
            reading(message_id="old-1", measured_at=old, value=9.0),
            reading(message_id="old-2", measured_at=old, value=2.0),
        ],
    )

    merge_files(spark, table, [late], "run-2")

    assert table_rows(spark, table) == [
        ("old-1", "temperature", 1.0),
        ("old-2", "temperature", 2.0),
        ("recent-1", "temperature", 1.0),
    ]


def test_same_key_in_another_hour_is_outside_the_matching_range(spark: SparkSession, table: str, tmp_path: Path):
    first = write_staged(spark, tmp_path / "a", [reading(device_code="code-a", measured_at=utc(11, 30), value=1.0)])
    merge_files(spark, table, [first], "run-1")
    moved = write_staged(spark, tmp_path / "b", [reading(device_code="code-a", measured_at=utc(14, 30), value=2.0)])

    merge_files(spark, table, [moved], "run-2")

    assert table_rows(spark, table) == [("msg-1", "temperature", 1.0), ("msg-1", "temperature", 2.0)]


def test_same_key_in_another_device_code_is_outside_the_matching_range(spark: SparkSession, table: str, tmp_path: Path):
    first = write_staged(spark, tmp_path / "a", [reading(device_code="code-a", measured_at=utc(11, 30), value=1.0)])
    merge_files(spark, table, [first], "run-1")
    moved = write_staged(spark, tmp_path / "b", [reading(device_code="code-b", measured_at=utc(11, 30), value=2.0)])

    merge_files(spark, table, [moved], "run-2")

    assert table_rows(spark, table) == [("msg-1", "temperature", 1.0), ("msg-1", "temperature", 2.0)]


def test_matching_range_pairs_each_device_code_with_its_own_hours_not_every_combination(
    spark: SparkSession, table: str, tmp_path: Path
):
    # Stored: (code-a, 14h). Incoming: (code-a, 11h) and (code-b, 14h). The range holds the pairs
    # (code-a, 11h) and (code-b, 14h) only; a cross product would also hold (code-a, 14h) and wrongly
    # match the stored row against the incoming (code-a, 11h) row.
    stored = write_staged(spark, tmp_path / "a", [reading(device_code="code-a", measured_at=utc(14, 30), value=1.0)])
    merge_files(spark, table, [stored], "run-1")
    incoming = write_staged(
        spark,
        tmp_path / "b",
        [
            reading(device_code="code-a", measured_at=utc(11, 30), value=2.0),
            reading(device_code="code-b", measured_at=utc(14, 30), value=3.0),
        ],
    )

    merge_files(spark, table, [incoming], "run-2")

    assert table_rows(spark, table) == [
        ("msg-1", "temperature", 1.0),
        ("msg-1", "temperature", 2.0),
        ("msg-1", "temperature", 3.0),
    ]


def test_message_spanning_hours_is_dropped_as_a_whole_when_resent(spark: SparkSession, table: str, tmp_path: Path):
    spanning = [
        reading(metric="temperature", value=1.0, measured_at=utc(11, 59)),
        reading(metric="humidity", value=2.0, measured_at=utc(12, 1)),
    ]
    first = write_staged(spark, tmp_path / "a", spanning)
    merge_files(spark, table, [first], "run-1")
    resent = write_staged(spark, tmp_path / "b", [{**r, "written_at": utc(12, 5)} for r in spanning])

    merge_files(spark, table, [resent], "run-2")

    assert table_rows(spark, table) == [("msg-1", "humidity", 2.0), ("msg-1", "temperature", 1.0)]


def test_message_spanning_hours_with_two_copies_in_one_run_keeps_one_copy(
    spark: SparkSession, table: str, tmp_path: Path
):
    spanning = [
        reading(metric="temperature", value=1.0, measured_at=utc(11, 59)),
        reading(metric="humidity", value=2.0, measured_at=utc(12, 1)),
    ]
    a = write_staged(spark, tmp_path / "a", spanning)
    b = write_staged(spark, tmp_path / "b", [{**r, "written_at": utc(12, 5)} for r in spanning])

    merge_files(spark, table, [a, b], "run-1")

    assert spark.table(table).count() == 2


def test_every_row_of_one_run_has_the_same_merge_run_id(spark: SparkSession, table: str, tmp_path: Path):
    path = write_staged(
        spark,
        tmp_path / "a",
        [reading(message_id="m1"), reading(message_id="m2", device_code="code-b"), reading(message_id="m3")],
    )

    merge_files(spark, table, [path], "20261030T120500Z-1a2b3c4d")

    assert {r["merge_run_id"] for r in spark.table(table).collect()} == {"20261030T120500Z-1a2b3c4d"}


def test_run_ids_have_the_documented_shape_and_sort_by_time():
    earlier = new_run_id(datetime(2026, 10, 30, 12, 5, 0, tzinfo=timezone.utc))
    later = new_run_id(datetime(2026, 10, 30, 12, 10, 0, tzinfo=timezone.utc))

    assert re.fullmatch(r"20261030T120500Z-[0-9a-f]{8}", earlier)
    assert earlier < later


def test_run_ids_of_the_same_instant_differ():
    now = datetime(2026, 10, 30, 12, 5, 0, tzinfo=timezone.utc)

    assert new_run_id(now) != new_run_id(now)


def test_insert_only_merge_appends_without_rewriting_existing_files(spark: SparkSession, table: str, tmp_path: Path):
    first = write_staged(spark, tmp_path / "a", [reading(message_id="m1"), reading(message_id="m2")])
    merge_files(spark, table, [first], "run-1")
    files_before = {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}
    second = write_staged(spark, tmp_path / "b", [reading(message_id="m1"), reading(message_id="m3")])

    merge_files(spark, table, [second], "run-2")

    snapshots = spark.sql(f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at").collect()
    files_after = {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}
    assert [s["operation"] for s in snapshots] == ["append", "append"]
    assert snapshots[-1]["summary"].get("deleted-data-files", "0") == "0"
    assert files_before < files_after


def test_table_stays_format_version_2_after_merge(spark: SparkSession, table: str, tmp_path: Path):
    path = write_staged(spark, tmp_path / "a", [reading()])

    merge_files(spark, table, [path], "run-1")

    props = {row["key"]: row["value"] for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert props["format-version"] == "2"


def test_match_condition_binds_each_device_code_and_names_each_hour_range():
    hour_a = int(datetime(2026, 10, 30, 11, tzinfo=timezone.utc).timestamp()) // 3600
    hour_b = int(datetime(2026, 10, 20, 3, tzinfo=timezone.utc).timestamp()) // 3600

    condition, parameters = match_condition([("code-a", hour_a), ("code-b", hour_b)])

    assert sorted(parameters.values()) == ["code-a", "code-b"]
    assert "code-a" not in condition
    for hour in (hour_a, hour_b):
        assert f"timestamp_micros({hour * 3_600_000_000})" in condition
        assert f"timestamp_micros({(hour + 1) * 3_600_000_000})" in condition


def test_device_code_with_quote_and_backslash_cannot_break_out_of_the_literal(
    spark: SparkSession, table: str, tmp_path: Path
):
    code = "it's\\a' OR 1=1 --"
    first = write_staged(spark, tmp_path / "a", [reading(device_code=code)])
    merge_files(spark, table, [first], "run-1")
    resent = write_staged(spark, tmp_path / "b", [reading(device_code=code, written_at=utc(12, 5))])

    merge_files(spark, table, [resent], "run-2")

    assert [r["device_code"] for r in spark.table(table).collect()] == [code]


def test_hours_are_aligned_to_utc_whatever_the_session_time_zone(spark: SparkSession, table: str, tmp_path: Path):
    first = write_staged(spark, tmp_path / "a", [reading(measured_at=utc(11, 30))])
    merge_files(spark, table, [first], "run-1")
    resent = write_staged(spark, tmp_path / "b", [reading(measured_at=utc(11, 30), written_at=utc(12, 5))])
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    try:
        merge_files(spark, table, [resent], "run-2")
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    assert spark.table(table).count() == 1


@pytest.mark.parametrize(
    "code", ["x${env:HOME}", "x${hiveconf:user.name}", "x${spark.app.name}", "$${env:HOME}", "x${"]
)
def test_variable_references_in_device_code_are_kept_literal(
    spark: SparkSession, table: str, tmp_path: Path, code: str
):
    first = write_staged(spark, tmp_path / "a", [reading(device_code=code)])
    merge_files(spark, table, [first], "run-1")
    resent = write_staged(spark, tmp_path / "b", [reading(device_code=code, written_at=utc(12, 5))])

    merge_files(spark, table, [resent], "run-2")

    assert [r["device_code"] for r in spark.table(table).collect()] == [code]


def test_merge_leaves_variable_substitution_setting_as_it_was(spark: SparkSession, table: str, tmp_path: Path):
    path = write_staged(spark, tmp_path / "a", [reading()])
    before = spark.conf.get("spark.sql.variable.substitute")

    merge_files(spark, table, [path], "run-1")

    assert spark.conf.get("spark.sql.variable.substitute") == before
