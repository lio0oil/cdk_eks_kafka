import base64
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    for records in event["records"].values():
        for record in records:
            value = base64.b64decode(record["value"]).decode("utf-8", errors="replace")
            logger.info(
                json.dumps(
                    {
                        "topic": record["topic"],
                        "partition": record["partition"],
                        "offset": record["offset"],
                        "value": value,
                    },
                    ensure_ascii=False,
                )
            )
