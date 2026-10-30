import json
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from botocore.client import BaseClient
from botocore.exceptions import ClientError

DEFAULT_MARGIN_SECONDS = 120
WATERMARK_KEY = "state/watermark.json"
MINUTE_FORMAT = "%Y%m%dT%H%M"
INCOMING_PREFIX = "incoming/"


def compute_cutoff(now: datetime, margin_seconds: int = DEFAULT_MARGIN_SECONDS) -> datetime:
    return (now - timedelta(seconds=margin_seconds)).astimezone(timezone.utc).replace(second=0, microsecond=0)


def _minute_prefix(minute: datetime) -> str:
    return f"{INCOMING_PREFIX}t={minute.strftime(MINUTE_FORMAT)}/"


def _parse_minute(prefix: str) -> datetime:
    stamp = prefix.removeprefix(f"{INCOMING_PREFIX}t=").rstrip("/")
    return datetime.strptime(stamp, MINUTE_FORMAT).replace(tzinfo=timezone.utc)


def read_watermark(s3: BaseClient, bucket: str) -> datetime | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=WATERMARK_KEY)["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return datetime.strptime(json.loads(body)["watermark"], MINUTE_FORMAT).replace(tzinfo=timezone.utc)


def write_watermark(s3: BaseClient, bucket: str, minute: datetime) -> None:
    body = json.dumps({"watermark": minute.strftime(MINUTE_FORMAT)}).encode()
    s3.put_object(Bucket=bucket, Key=WATERMARK_KEY, Body=body)


def _minute_prefixes(s3: BaseClient, options: dict[str, str]) -> Iterator[str]:
    for page in s3.get_paginator("list_objects_v2").paginate(**options):
        for entry in page.get("CommonPrefixes", []):
            yield entry["Prefix"]


def list_incoming(s3: BaseClient, bucket: str, watermark: datetime | None, cutoff: datetime) -> list[str]:
    """Lists the files of minutes after the watermark and before the cutoff, oldest minute first.

    The cutoff minute is excluded: the writer labels a file with the minute its invocation started, and the PUT
    can come later, so only minutes before the cutoff are guaranteed to receive no more files.
    """
    options = {"Bucket": bucket, "Prefix": INCOMING_PREFIX, "Delimiter": "/"}
    if watermark is not None:
        # The watermark minute itself is still returned (its keys sort after this), so it is filtered below.
        options["StartAfter"] = _minute_prefix(watermark)
    minute_prefixes = []
    for prefix in _minute_prefixes(s3, options):
        minute = _parse_minute(prefix)
        if minute >= cutoff:
            break
        if watermark is None or minute > watermark:
            minute_prefixes.append(prefix)
    return [
        f"s3://{bucket}/{obj['Key']}"
        for prefix in minute_prefixes
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
