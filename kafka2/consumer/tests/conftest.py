import os

os.environ.setdefault("CHECKPOINT_BUCKET", "test-checkpoint-bucket")
os.environ.setdefault("OUTPUT_BUCKET", "test-output-bucket")

from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    session = SparkSession.builder.master("local[2]").appName("consumer-test").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    yield session
    session.stop()
