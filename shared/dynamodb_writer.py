"""DynamoDB persistence for One-Click Agent analysis results.

Writes the five fields required by the 4/21 MOMs into a DynamoDB table:
    User, DateTime, RecordId, ErrorMessage, RootCause

Design goals:
- Never raises to the caller. DynamoDB failures must not break the agent
  stream back to the end user.
- Configurable via environment (``ONECLICK_TABLE_NAME``, ``AWS_REGION``)
  with safe defaults so it works as a standalone script.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

DEFAULT_TABLE_NAME = "OneClick-Agent-Results"
DEFAULT_REGION = "us-east-1"


def _get_table(table_name: Optional[str] = None, region: Optional[str] = None):
    name = table_name or os.environ.get("ONECLICK_TABLE_NAME", DEFAULT_TABLE_NAME)
    region = region or os.environ.get("AWS_REGION", DEFAULT_REGION)
    resource = boto3.resource("dynamodb", region_name=region)
    return resource.Table(name), name, region


def _convert_floats(obj):
    """Recursively convert float values to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _convert_floats(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_floats(v) for v in obj]
    return obj


async def save_analysis_result(
    user: str,
    datetime_str: str,
    record_id: str,
    error_message: str,
    root_cause: str | dict,
    error_code: str = "",
    table_name: Optional[str] = None,
    region: Optional[str] = None,
) -> dict:
    """Persist one analysis result row asynchronously via thread pool.

    Args:
        user: Agent LAN ID. Used as partition key.
        datetime_str: Report trigger time (ISO 8601). Used as sort key.
        record_id: Salesforce Vlocity Error Log record ID (may be empty).
        error_message: Raw error message from the report (may be empty).
        root_cause: The root-cause analysis text produced by the agent.
        error_code: The error code retrieved from Datadog or parsed result (e.g. "400").
        table_name: Override the table name (else env ``ONECLICK_TABLE_NAME``).
        region: Override the AWS region (else env ``AWS_REGION``).

    Returns:
        ``{"success": True, "table": ..., "key": {...}}`` on success,
        or ``{"success": False, "error": "..."}`` on failure.
    """
    # Defensive: DynamoDB string attributes cannot be empty. Substitute a
    # placeholder so put_item never rejects the row.
    user_val = (user or "").strip() or "UNKNOWN"
    dt_val = (datetime_str or "").strip() or datetime.now(timezone.utc).isoformat()

    item = {
        "User": user_val,
        "DateTime": dt_val,
        "RecordId": record_id or "",
        "ErrorMessage": error_message or "",
        "RootCause": _convert_floats(root_cause) if root_cause else "",
        "ErrorCode": error_code or "",
        "CreatedAt": datetime.now(timezone.utc).isoformat(),
    }

    try:
        loop = asyncio.get_event_loop()
        table, name, region_used = _get_table(table_name, region)
        
        # boto3 is blocking, so we run the put_item in a thread pool to avoid blocking the event loop.
        await loop.run_in_executor(None, lambda: table.put_item(Item=item))
        
        logger.info(
            "Wrote analysis result to DynamoDB table=%s user=%s datetime=%s",
            name,
            user_val,
            dt_val,
        )
        return {
            "success": True,
            "table": name,
            "region": region_used,
            "key": {"User": user_val, "DateTime": dt_val},
        }
    except (ClientError, BotoCoreError) as e:
        logger.error("DynamoDB put_item failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:  # pragma: no cover - defensive
        logger.exception("Unexpected error writing to DynamoDB")
        return {"success": False, "error": str(e)}
