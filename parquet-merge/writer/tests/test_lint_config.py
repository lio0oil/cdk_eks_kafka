import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_ruff_rejects_unittest_mock(tmp_path: Path):
    source = tmp_path / "uses_unittest_mock.py"
    source.write_text("from unittest import mock\n\nmock.Mock()\n")

    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "--config", str(PROJECT_ROOT / "pyproject.toml"), str(source)],
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "TID251" in result.stdout


def _run_ruff_on_generated_file(*args: str) -> subprocess.CompletedProcess[str]:
    # pre-commit はファイル名を明示して渡すため、ディレクトリ探索時のみ効く exclude では防げない
    return subprocess.run(
        [sys.executable, "-m", "ruff", *args, "--config", str(PROJECT_ROOT / "pyproject.toml"), "readings_pb2.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )


def test_ruff_check_ignores_generated_file_passed_explicitly():
    result = _run_ruff_on_generated_file("check", "--select", "E,F,I")

    assert result.returncode == 0, result.stdout + result.stderr


def test_ruff_format_check_ignores_generated_file_passed_explicitly():
    result = _run_ruff_on_generated_file("format", "--check")

    assert result.returncode == 0, result.stdout + result.stderr
