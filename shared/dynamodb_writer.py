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
import json
import logging
import os
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Optional

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


def _to_dynamodb_document(value: Any) -> Any:
    """Recursively convert Python values to DynamoDB-safe document values."""
    if isinstance(value, float):
        # DynamoDB does not accept float; use Decimal for numeric fidelity.
        return Decimal(str(value))
    if isinstance(value, dict):
        return {str(k): _to_dynamodb_document(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_dynamodb_document(v) for v in value]
    if isinstance(value, tuple):
        return [_to_dynamodb_document(v) for v in value]
    return value


def _normalize_root_cause(root_cause: Any) -> dict[str, Any]:
    """Normalize input into a JSON object for DynamoDB Map storage."""
    if isinstance(root_cause, dict):
        payload = root_cause
    elif isinstance(root_cause, str):
        text = root_cause.strip()
        if not text:
            payload = {}
        else:
            try:
                parsed = json.loads(text)
                payload = parsed if isinstance(parsed, dict) else {"raw_text": text}
            except json.JSONDecodeError:
                payload = {"raw_text": text}
    elif root_cause is None:
        payload = {}
    else:
        payload = {"raw_value": str(root_cause)}

    return _to_dynamodb_document(payload)


async def save_analysis_result(
    user: str,
    datetime_str: str,
    record_id: str,
    error_message: str,
    root_cause: Any,
    error_code: str,
    mulesoft_logs: Optional[dict] = None,
    table_name: Optional[str] = None,
    region: Optional[str] = None,
) -> dict:
    """Persist one analysis result row asynchronously via thread pool.

    Args:
        user: Agent LAN ID. Used as partition key.
        datetime_str: Report trigger time (ISO 8601). Used as sort key.
        record_id: Salesforce Vlocity Error Log record ID (may be empty).
        error_message: Raw error message from the report (may be empty).
        root_cause: Root-cause JSON object (or JSON/raw text to normalize).
        error_code: Error code classification (may be empty).
        mulesoft_logs: Mulesoft debug logs (Log 2) to persist.
        table_name: Override the table name.
        region: Override the AWS region.
    """
    # Defensive: DynamoDB string attributes cannot be empty. Substitute a
    # placeholder so put_item never rejects the row.
    user_val = (user or "").strip() or "UNKNOWN"
    dt_val = (datetime_str or "").strip() or datetime.now(timezone.utc).isoformat()
    root_cause_val = _normalize_root_cause(root_cause)

    item = {
        "User": user_val,
        "DateTime": dt_val,
        "RecordId": record_id or "",
        "ErrorMessage": error_message or "",
        "RootCause": root_cause_val,
        "ErrorCode": error_code or "",
        "CreatedAt": datetime.now(timezone.utc).isoformat(),
    }

    if mulesoft_logs:
        item["MulesoftLogs"] = _to_dynamodb_document(mulesoft_logs)

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
