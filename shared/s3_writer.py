"""S3 persistence for One-Click Agent per-RecordID artifacts.

Writes three JSON artifacts to S3 under a per-RecordID prefix:
    <prefix>/raw_data.json          — original report payload from One-Click
    <prefix>/session_log_data.json  — last-10-min Datadog records for the LAN ID
    <prefix>/analysis_results.json  — Bedrock analysis (parsed JSON output)

Design goals (mirror dynamodb_writer.py):
- Never raises to the caller. S3 failures must not break the agent stream.
- Async via thread pool around the blocking boto3 client.
- Configurable via environment (``ONECLICK_S3_BUCKET``, ``AWS_REGION``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

DEFAULT_BUCKET_NAME = "oneclick-agent-artifacts"
DEFAULT_REGION = "us-east-1"

# S3 keys are permissive but we still strip characters that are awkward for
# downstream consumers / URL-safe paths.
_UNSAFE_KEY_CHARS = re.compile(r"[^A-Za-z0-9._\-]+")


def _sanitize(component: str) -> str:
    """Sanitize a single S3 key path component."""
    cleaned = _UNSAFE_KEY_CHARS.sub("_", (component or "").strip())
    return cleaned.strip("._-") or "UNKNOWN"


def _build_prefix(record_id: str, user: str, datetime_str: str) -> str:
    """Choose the S3 prefix: record_id when present, else {user}_{datetime}."""
    rid = (record_id or "").strip()
    if rid:
        return _sanitize(rid)
    user_part = _sanitize(user or "UNKNOWN")
    dt_part = _sanitize(datetime_str or datetime.now(timezone.utc).isoformat())
    return f"{user_part}_{dt_part}"


def _dump(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)


async def save_oneclick_artifacts(
    record_id: str,
    user: str,
    datetime_str: str,
    raw_data: Optional[Any] = None,
    session_log_data: Optional[Any] = None,
    analysis_results: Optional[Any] = None,
    mulesoft_logs: Optional[Any] = None,
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
) -> dict:
    """Persist the JSON artifacts to S3 asynchronously.

    Args:
        record_id: Salesforce Vlocity Error Log record ID.
        user: Agent LAN ID.
        datetime_str: Report trigger time.
        raw_data: Original One-Click report payload.
        session_log_data: Last 10-minute Datadog records.
        analysis_results: Bedrock analysis results.
        mulesoft_logs: Mulesoft debug logs (Log 2).
        bucket_name: Override the bucket.
        region: Override the region.
    """
    bucket = bucket_name or os.environ.get("ONECLICK_S3_BUCKET", DEFAULT_BUCKET_NAME)
    region_used = region or os.environ.get("AWS_REGION", DEFAULT_REGION)
    prefix = _build_prefix(record_id, user, datetime_str)

    artifacts: list[tuple[str, Any]] = []
    if raw_data is not None:
        artifacts.append(("raw_data.json", raw_data))
    if session_log_data is not None:
        artifacts.append(("session_log_data.json", session_log_data))
    if analysis_results is not None:
        artifacts.append(("analysis_results.json", analysis_results))
    if mulesoft_logs is not None:
        artifacts.append(("mulesoft_logs.json", mulesoft_logs))

    if not artifacts:
        return {"success": True, "bucket": bucket, "prefix": prefix, "keys": []}

    try:
        client = boto3.client("s3", region_name=region_used)
        loop = asyncio.get_event_loop()
        written: list[str] = []

        for filename, payload in artifacts:
            key = f"{prefix}/{filename}"
            body = _dump(payload).encode("utf-8")
            await loop.run_in_executor(
                None,
                lambda k=key, b=body: client.put_object(
                    Bucket=bucket,
                    Key=k,
                    Body=b,
                    ContentType="application/json",
                ),
            )
            written.append(key)

        logger.info(
            "Wrote %d One-Click artifacts to s3://%s/%s/",
            len(written),
            bucket,
            prefix,
        )
        return {
            "success": True,
            "bucket": bucket,
            "region": region_used,
            "prefix": prefix,
            "keys": written,
        }
    except (ClientError, BotoCoreError) as e:
        logger.error("S3 put_object failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:  # pragma: no cover - defensive
        logger.exception("Unexpected error writing One-Click artifacts to S3")
        return {"success": False, "error": str(e)}
