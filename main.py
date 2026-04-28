"""One-Click Report Analysis Agent — Strands on AgentCore Runtime.

Defines the agent with @tool-decorated functions for report parsing,
log lookups, and issue classification. Deployed on AgentCore Runtime.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from typing import Any, Optional
import boto3
from botocore.exceptions import ClientError

from strands import Agent, tool
from bedrock_agentcore.runtime import BedrockAgentCoreApp

from model.load import load_model
from tools.parse_report import parse_report as _parse_report, VLOCITY_ID_PATTERN, EXCEPTION_ID_PATTERN
from tools.classify_issue import classify_issue as _classify_issue
from tools.query_vlocity_logs import (
    get_vlocity_log_by_id as _get_vlocity_log,
    search_vlocity_logs as _search_vlocity,
)
from tools.query_exception_logs import (
    get_exception_log_by_id as _get_exception_log,
    search_exception_logs as _search_exceptions,
)
from tools.query_mulesoft_logs import query_mulesoft_logs
from shared.dynamodb_writer import save_analysis_result
from shared.s3_writer import save_oneclick_artifacts


app = BedrockAgentCoreApp()
log = app.logger


# ---------------------------------------------------------------------------
# Datadog Credentials Management
# ---------------------------------------------------------------------------

_datadog_credentials = None


def get_datadog_credentials() -> dict:
    """Fetch Datadog credentials from AWS Secrets Manager.
    
    Returns:
        dict: Contains 'api_key', 'application_key', and 'endpoint'
    """
    global _datadog_credentials
    
    # Return cached credentials if available
    if _datadog_credentials is not None:
        return _datadog_credentials
    
    secret_name = os.environ.get('DATADOG_SECRET_NAME', 'temp_datadog_credentials')
    region_name = os.environ.get('AWS_REGION', 'us-east-1')
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        log.info(f"Fetching Datadog credentials from Secrets Manager: {secret_name}")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            _datadog_credentials = {
                'api_key': secret.get('DD_API_KEY', ''),
                'application_key': secret.get('DD_APPLICATION_KEY', ''),
                'endpoint': secret.get('DD_ENDPOINT', 'https://api.datadoghq.com/api/v2/logs/events/search')
            }
            log.info("Successfully fetched Datadog credentials")
            return _datadog_credentials
        else:
            log.error("Secret does not contain SecretString")
            raise ValueError("Secret format is invalid")
            
    except ClientError as e:
        log.error(f"Error fetching secret: {e}")
        raise
    except Exception as e:
        log.error(f"Unexpected error fetching Datadog credentials: {e}")
        raise


# ---------------------------------------------------------------------------
# Tools — @tool wrappers around the module implementations
# ---------------------------------------------------------------------------

@tool
def parse_report(
    user: str,
    datetime: str = "",
    processidentifier: str = "",
    errormessage: str = "",
    description: str = "",
) -> dict:
    """Parse a One-Click Report to extract structured data and identify
    embedded Salesforce record IDs (Vlocity Error Log IDs starting with
    'a9z' and PS Exception Log IDs starting with 'a1W').

    Args:
        user: Agent LAN ID.
        datetime: Report timestamp (ISO 8601).
        processidentifier: OmniScript process identifier.
        errormessage: Raw error message text.
        description: Agent-entered description of the issue.
    """
    return _parse_report({
        "user": user,
        "datetime": datetime,
        "processidentifier": processidentifier,
        "errormessage": errormessage,
        "description": description,
    })


@tool
def get_vlocity_log_by_id(log_id: str) -> dict:
    """Look up a Vlocity Error Log by its Salesforce record ID. Returns the
    full log including HTTP request/response payloads with parsed error details.

    Args:
        log_id: Salesforce Vlocity Error Log ID (starts with 'a9z').
    """
    return _get_vlocity_log(log_id)


@tool
def search_vlocity_logs(user: str, start_time: str, end_time: str) -> dict:
    """Search for Vlocity Error Logs by agent LAN ID and time range. Use when
    no direct log IDs are available in the report. Search +/- 30 minutes around
    the report timestamp.

    Args:
        user: Agent LAN ID.
        start_time: Start of time window (ISO 8601).
        end_time: End of time window (ISO 8601).
    """
    return _search_vlocity(user, start_time, end_time)


@tool
def get_exception_log_by_id(log_id: str) -> dict:
    """Look up a PS Exception Log by its Salesforce record ID. Returns
    exception type, severity, location, and error message.

    Args:
        log_id: Salesforce PS Exception Log ID (starts with 'a1W').
    """
    return _get_exception_log(log_id)


@tool
def search_exception_logs(application: str = "", location: str = "") -> dict:
    """Search PS Exception Logs by application name and/or exception location
    (class name). Use to find related exceptions when you know the application
    or error location.

    Args:
        application: Application name (e.g., 'CCSP').
        location: Exception location / class name (e.g., 'CCSP_IP_GetRatesFlyoutInfo').
    """
    return _search_exceptions(application=application, location=location)


@tool
def classify_issue(description: str, error_data: str = "") -> dict:
    """Classify an issue based on the agent's description text and any error
    data gathered from log lookups. Returns category (LATENCY, UI_ERROR,
    AUTH_ERROR, DATA_ERROR, UNKNOWN), confidence score, and whether recording
    review is needed.

    Args:
        description: Agent-entered description of the issue.
        error_data: Error data gathered from log lookups (optional).
    """
    return _classify_issue(description, error_data)


@tool
async def query_datadog_error_logs(record_id: str, user: str = "", trigger_time: str = "", lookback_minutes: int = 1440) -> dict:
    """Query Datadog for Vlocity error log entries matching a specific record ID.
    Filters for error status codes only (non-200). Returns the full error log
    payload including HTTP request/response, error code, functionality, and
    user details from the Datadog CDC event stream.

    Args:
        record_id: Salesforce Vlocity Error Log record ID (starts with 'a9z').
        user: Agent LAN ID / alias (optional, used to narrow results).
        trigger_time: Trigger timestamp (ISO 8601 format, optional).
        lookback_minutes: Minutes to look back from trigger time (default: 1440).
    """
    try:
        import httpx

        credentials = get_datadog_credentials()
        api_key = credentials['api_key']
        app_key = credentials['application_key']
        endpoint = credentials['endpoint']

        headers = {
            'DD-API-KEY': api_key,
            'DD-APPLICATION-KEY': app_key,
            'Content-Type': 'application/json'
        }

        # Determine time range
        if trigger_time:
            trigger_dt = datetime.fromisoformat(trigger_time.replace('Z', '+00:00'))
        else:
            trigger_dt = datetime.now()
        start_dt = trigger_dt - timedelta(minutes=lookback_minutes)

        # Error codes to query
        error_codes = [400, 401, 403, 404, 408, 413, 429, 500, 502, 503, 504]

        all_matches = []

        for code in error_codes:
            query = (
                "env_type:production "
                "@data.payload.ChangeEventHeader.entityName:"
                "vlocity_cmt__VlocityErrorLogEntry__c "
                f"@data.payload.vlocity_cmt__ErrorCode__c:{code}"
            )

            body = {
                'filter': {
                    'from': start_dt.isoformat(),
                    'to': trigger_dt.isoformat(),
                    'query': query,
                },
                'sort': 'timestamp',
                'page': {'limit': 100},
            }

            log.info(f"Querying Datadog for error code {code}, record_id={record_id}")

            async with httpx.AsyncClient() as client:
                response = await client.post(endpoint, headers=headers, json=body, timeout=30.0)
                response.raise_for_status()
                data = response.json()
                events = data.get('data', [])

            # Filter events by user (LanID) and record_id if provided
            for event in events:
                attrs = event.get('attributes', {}).get('attributes', {})
                user_role = attrs.get('user_role', {})
                user_alias = user_role.get('alias', '')

                # Primary filter: Match by User (LanID) if provided
                if user and user_alias != user:
                    continue

                payload = attrs.get('data', {}).get('payload', {})
                # Primary filter: Match by User (LanID) if provided
                if user and user_alias != user:
                    continue

                http_response = payload.get('CCSP_HTTPResponse__c', '')
                http_request = payload.get('CCSP_HTTPRequest__c', '')
                user_profile = attrs.get('user_profile', {})

                # Parse HTTP response JSON if possible
                parsed_response = {}
                if http_response:
                    try:
                        parsed_response = json.loads(http_response)
                    except (json.JSONDecodeError, TypeError):
                        parsed_response = {'raw': http_response}

                all_matches.append({
                    'record_id': record_id,
                    'lan_id': user_alias,  # Added lan_id to match datadog_retrieve.py pattern
                    'name': payload.get('Name', ''),
                    'error_code': str(payload.get('vlocity_cmt__ErrorCode__c', '')),
                    'functionality': payload.get('CCSP_Functionality__c', ''),
                    'callout_status': payload.get('CCSP_CalloutStatus__c', ''),
                    'log_number': payload.get('CCSP_LogNumber__c', ''),
                    'object_name': payload.get('vlocity_cmt__ObjectName__c', ''),
                    'os_type': payload.get('CCSP_OS_Type__c', ''),
                    'status': payload.get('CCSP_Status__c', ''),
                    'context_id': payload.get('vlocity_cmt__ContextId__c', ''),
                    'source_name': payload.get('vlocity_cmt__SourceName__c', ''),
                    'source_type': payload.get('vlocity_cmt__SourceType__c', ''),
                    'request_sent_time': payload.get('CCSP_Request_Sent_Time__c', ''),
                    'response_received_time': payload.get('CCSP_Response_Received_Time__c', ''),
                    'created_date': payload.get('CreatedDate', ''),
                    'http_request': http_request,
                    'http_response': parsed_response,
                    'record_type_id': payload.get('RecordTypeId', ''),
                    'user_profile_name': user_profile.get('name', ''),
                    'user_profile_type': user_profile.get('profile_name', ''),
                    'user_alias': user_alias,
                    'user_role_name': user_role.get('role_name', ''),
                    'timestamp': event.get('attributes', {}).get('timestamp', ''),
                })

        log.info(f"Found {len(all_matches)} matching error logs for user={user} record_id={record_id}")

        return {
            'success': True,
            'record_id': record_id,
            'user': user,
            'trigger_time': trigger_time,
            'match_count': len(all_matches),
            'error_logs': all_matches,
            'time_range': {
                'start': start_dt.isoformat(),
                'end': trigger_dt.isoformat(),
            },
        }

    except Exception as e:
        log.error(f"Error querying Datadog: {e}")
        return {
            'success': False,
            'error': str(e),
            'record_id': record_id,
            'user': user,
            'trigger_time': trigger_time,
        }


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "system_prompt.txt")


def _load_system_prompt() -> str:
    with open(_SYSTEM_PROMPT_PATH) as f:
        return f.read()


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

ALL_TOOLS = [
    parse_report,
    get_vlocity_log_by_id,
    search_vlocity_logs,
    get_exception_log_by_id,
    search_exception_logs,
    classify_issue,
    query_datadog_error_logs,
]

_agent = None


def get_or_create_agent() -> Agent:
    global _agent
    if _agent is None:
        _agent = Agent(
            model=load_model(),
            system_prompt=_load_system_prompt(),
            tools=ALL_TOOLS,
        )
    return _agent


# ---------------------------------------------------------------------------
# Datadog fetch helper
# ---------------------------------------------------------------------------

_DD_ERROR_CODES = [
    300, 301, 302, 303, 304, 305, 306, 307, 308,
    400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 421, 422, 423, 424, 425, 426, 427, 428, 429, 431, 440, 444, 449, 450, 451, 460, 463, 494, 495, 496, 497, 498, 499,
    500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511, 520, 521, 522, 523, 524, 525, 526, 527, 530, 561, 598, 599
]


async def _fetch_datadog_logs(
    user: str,
    record_id: str,
    trigger_dt: datetime,
    lookback_minutes: int,
) -> dict:
    """Query Datadog for Vlocity error log events filtered by LAN ID.

    Returns the same dict shape previously produced inline in ``invoke()``.
    Never raises; failures are returned as ``{"success": False, ...}``.
    """
    try:
        import httpx

        credentials = get_datadog_credentials()
        api_key = credentials['api_key']
        app_key = credentials['application_key']
        endpoint = credentials['endpoint']

        start_dt = trigger_dt - timedelta(minutes=lookback_minutes)
        headers = {
            'DD-API-KEY': api_key,
            'DD-APPLICATION-KEY': app_key,
            'Content-Type': 'application/json',
        }

        matched_logs: list[dict] = []

        for code in _DD_ERROR_CODES:
            query = (
                "env_type:production "
                "@data.payload.ChangeEventHeader.entityName:"
                "vlocity_cmt__VlocityErrorLogEntry__c "
                f"@data.payload.vlocity_cmt__ErrorCode__c:{code}"
            )
            request_body = {
                'filter': {
                    'from': start_dt.isoformat(),
                    'to': trigger_dt.isoformat(),
                    'query': query,
                },
                'sort': 'timestamp',
                'page': {'limit': 100},
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(endpoint, headers=headers, json=request_body, timeout=30.0)
                response.raise_for_status()
                data = response.json()
                events = data.get('data', [])

            for event in events:
                attrs = event.get('attributes', {}).get('attributes', {})
                user_role = attrs.get('user_role', {})
                user_alias = user_role.get('alias', '')

                # Filter by LAN ID when provided.
                if user and user_alias != user:
                    continue

                dd_payload = attrs.get('data', {}).get('payload', {})

                http_response = dd_payload.get('CCSP_HTTPResponse__c', '')
                parsed_response: Any = {}
                if http_response:
                    try:
                        parsed_response = json.loads(http_response)
                    except (json.JSONDecodeError, TypeError):
                        parsed_response = {'raw': http_response}

                user_profile = attrs.get('user_profile', {})

                matched_logs.append({
                    'record_id': record_id,
                    'lan_id': user_alias,
                    'name': dd_payload.get('Name', ''),
                    'error_code': str(dd_payload.get('vlocity_cmt__ErrorCode__c', '')),
                    'functionality': dd_payload.get('CCSP_Functionality__c', ''),
                    'callout_status': dd_payload.get('CCSP_CalloutStatus__c', ''),
                    'log_number': dd_payload.get('CCSP_LogNumber__c', ''),
                    'object_name': dd_payload.get('vlocity_cmt__ObjectName__c', ''),
                    'status': dd_payload.get('CCSP_Status__c', ''),
                    'context_id': dd_payload.get('vlocity_cmt__ContextId__c', ''),
                    'created_date': dd_payload.get('CreatedDate', ''),
                    'http_request': dd_payload.get('CCSP_HTTPRequest__c', ''),
                    'http_response': parsed_response,
                    'user_name': user_profile.get('name', ''),
                    'user_alias': user_alias,
                    'user_role_name': user_role.get('role_name', ''),
                    'timestamp': event.get('attributes', {}).get('timestamp', ''),
                })

        log.info(
            f"Datadog: {len(matched_logs)} logs for user={user} record_id={record_id} "
            f"lookback_minutes={lookback_minutes}"
        )
        return {
            'success': True,
            'record_id': record_id,
            'match_count': len(matched_logs),
            'error_logs': matched_logs,
            'time_range': {
                'start': start_dt.isoformat(),
                'end': trigger_dt.isoformat(),
            },
        }
    except Exception as e:
        log.error(f"Error fetching Datadog logs: {e}")
        return {
            'success': False,
            'error': str(e),
            'record_id': record_id,
        }


# ---------------------------------------------------------------------------
# Root-cause JSON extraction
# ---------------------------------------------------------------------------

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE)


def _extract_root_cause_json(text: str) -> Optional[dict]:
    """Extract a JSON object from the agent's streamed analysis text.

    Strategy:
        1. Try the first ```json``` (or generic ```) fenced code block.
        2. Otherwise scan for the first balanced ``{...}`` substring.
    Returns the parsed dict, or ``None`` if no valid JSON object is found.
    """
    if not text:
        return None

    # 1) Fenced block.
    match = _JSON_FENCE_RE.search(text)
    if match:
        try:
            obj = json.loads(match.group(1))
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass

    # 2) First balanced { ... } substring.
    start = text.find("{")
    while start != -1:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start:i + 1]
                    try:
                        obj = json.loads(candidate)
                        if isinstance(obj, dict):
                            return obj
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)

    return None


def GetResponseFromJSon(value: dict) -> str:
    """Extract ``error_code`` or ``code`` from data that appears after ``response:``."""
    if not isinstance(value, dict):
        return ""

    text = json.dumps(value)
    match = re.search(r"response\s*:\s*", text, re.IGNORECASE)
    if not match:
        return ""

    trimmed_text = text[match.end():].strip()

    # Handle escaped JSON payloads that can appear after serialization.
    unescaped_text = (
        trimmed_text
        .replace('\\"', '"')
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
    )

    response_json = _extract_root_cause_json(trimmed_text)
    log.info("response_json trimmed text: %s", response_json)

    if not response_json:
        response_json = _extract_root_cause_json(unescaped_text)
    if not response_json:
        return ""

    log.info("response_json unescaped text: %s", response_json)

    error_code = response_json.get('error_code')
    if error_code is not None and str(error_code).strip():
        log.info("error_code: %s", error_code)
        return str(error_code).strip()

    code = response_json.get('code')
    if code is not None and str(code).strip():
        log.info("code: %s", code)
        return str(code).strip()

    return ""


def _derive_error_code(datadog_logs: Optional[dict], root_cause_json: Optional[dict]) -> str:
    """Pick a single error code, preferring ``error_code`` then ``code``."""
    if datadog_logs and datadog_logs.get('success'):
        for entry in datadog_logs.get('error_logs') or []:
            error_code = str(entry.get('error_code') or '').strip()
            if error_code:
                return error_code

            code = str(entry.get('code') or '').strip()
            if code:
                return code

    if root_cause_json:
        error_code = root_cause_json.get('error_code')
        if error_code is not None and str(error_code).strip():
            return str(error_code).strip()

        code = root_cause_json.get('code')
        if code is not None and str(code).strip():
            return str(code).strip()

    return ""


# ---------------------------------------------------------------------------
# AgentCore Runtime entrypoint
# ---------------------------------------------------------------------------

@app.entrypoint
async def invoke(payload, context):
    """AgentCore Runtime entrypoint. Receives report data and streams analysis."""
    log.info("Invoking One-Click Analysis Agent...")

    # Unwrap the body if present (Lambda sends { "body": { ... } })
    body = payload.get("body", payload)

    # Extract key fields
    record_id = body.get("recordid", "")
    trigger_time = body.get("datetime", "")
    user = body.get("user", "")
    error_message = body.get("errormessage", "")

    # Per 4/21 MOMs: if no error message is present, pull the last 10 minutes
    # of Datadog data for that user (ignore record_id filter) and send to the
    # Agent as the normal flow would.
    no_error_fallback = not (error_message or "").strip()
    primary_lookback = 10 if no_error_fallback else 1440

    # Resolve trigger datetime (used for both Datadog fetches).
    trigger_dt: Optional[datetime] = None
    if trigger_time:
        try:
            trigger_dt = datetime.fromisoformat(trigger_time.replace('Z', '+00:00'))
        except ValueError:
            log.warning(f"Could not parse trigger_time={trigger_time!r}; using now()")

    # Proactively fetch the primary Datadog error logs (sent to the agent).
    datadog_error_logs: Optional[dict] = None
    should_fetch_datadog = trigger_dt is not None and (bool(record_id) or no_error_fallback)
    if should_fetch_datadog:
        log.info(
            f"Fetching primary Datadog logs user={user} record_id={record_id} "
            f"no_error_fallback={no_error_fallback} lookback_minutes={primary_lookback}"
        )
        datadog_error_logs = await _fetch_datadog_logs(
            user=user,
            record_id=record_id,
            trigger_dt=trigger_dt,
            lookback_minutes=primary_lookback,
        )

    log.info("Raw data: %s", datadog_error_logs)

    # Always fetch the last-10-min session log for the LAN ID (separate
    # artifact). When the primary fetch was already 10 min, reuse it.
    session_log_data: Optional[dict] = None
    if trigger_dt is not None and user:
        if datadog_error_logs is not None and primary_lookback == 10:
            session_log_data = datadog_error_logs
        else:
            session_log_data = await _fetch_datadog_logs(
                user=user,
                record_id=record_id,
                trigger_dt=trigger_dt,
                lookback_minutes=10,
            )

    log.info("Session log data: %s", session_log_data)

    # Correlate Context ID for Mulesoft logs (Log 2)
    mulesoft_logs: Optional[dict] = None
    if datadog_error_logs and datadog_error_logs.get('success'):
        for err_entry in datadog_error_logs.get('error_logs', []):
            full_ctx_id = err_entry.get('context_id')
            if full_ctx_id:
                # Extract the UUID part (after the last colon) if present
                # Example: "2026-04-27T17:06:40:8ee3825a..." -> "8ee3825a..."
                ctx_id = full_ctx_id.split(':')[-1] if ':' in full_ctx_id else full_ctx_id
                
                log.info(f"Correlating Mulesoft logs with Pivot ID: {ctx_id} (from {full_ctx_id})")
                
                credentials = get_datadog_credentials()
                mulesoft_logs = await query_mulesoft_logs(
                    ctx_id=ctx_id,
                    trigger_time=trigger_time,
                    credentials=credentials
                )
                break

    agent = get_or_create_agent()

    prompt = payload.get("prompt", "")
    if not prompt:
        # Build report data from the body fields
        report_data = dict(body)
        if datadog_error_logs:
            report_data['datadog_error_logs'] = datadog_error_logs
        if mulesoft_logs:
            report_data['mulesoft_debug_logs'] = mulesoft_logs

        prompt = (
            "Analyze the following One-Click Report entry and provide a full "
            "root cause analysis.\n\n"
            f"Report Data:\n{json.dumps(report_data, indent=2)}"
        )

    root_cause_buffer: list[str] = []
    stream = agent.stream_async(prompt)
    async for event in stream:
        if "data" in event and isinstance(event["data"], str):
            chunk = event["data"]
            root_cause_buffer.append(chunk)
            yield chunk

    # Post-stream persistence. Failures must never break the response stream
    # back to the caller.
    root_cause_text = "".join(root_cause_buffer).strip()
    root_cause_json = _extract_root_cause_json(root_cause_text)
    if root_cause_json is None:
        log.warning("Could not extract JSON root_cause from agent output; storing raw_text fallback")

    #error_code = _derive_error_code(datadog_error_logs, root_cause_json)
    error_code = _extract_root_cause_json(error_message).get('code', '') if error_message else ''

    log.info("Error Code: %s", error_code)
    root_cause_for_dynamodb: dict[str, Any] = (
        root_cause_json
        if root_cause_json is not None
        else ({"raw_text": root_cause_text} if root_cause_text else {})
    )

    # Attach Mulesoft Correlation info to DynamoDB RootCause
    if mulesoft_logs and mulesoft_logs.get('success'):
        root_cause_for_dynamodb['mulesoft_correlation'] = {
            'ctx_id': mulesoft_logs.get('ctx_id'),
            'log_count': len(mulesoft_logs.get('logs', []))
        }

    # 1) DynamoDB
    try:
        write_result = await save_analysis_result(
            user=user,
            datetime_str=trigger_time,
            record_id=record_id,
            error_message=error_message,
            root_cause=root_cause_for_dynamodb,
            error_code=error_code,
            mulesoft_logs=mulesoft_logs,
        )
        if write_result.get("success"):
            log.info("Persisted analysis result to DynamoDB: %s", write_result.get("key"))
        else:
            log.error("Failed to persist analysis result: %s", write_result.get("error"))
    except Exception as e:
        log.error(f"Unexpected error persisting analysis result: {e}")

    # 2) S3 artifacts (raw_data, session_log_data, analysis_results)
    try:
        analysis_payload: Any = (
            root_cause_json if root_cause_json is not None
            else ({"raw_text": root_cause_text} if root_cause_text else None)
        )
        s3_result = await save_oneclick_artifacts(
            record_id=record_id,
            user=user,
            datetime_str=trigger_time,
            raw_data=datadog_error_logs,
            session_log_data=session_log_data,
            mulesoft_logs=mulesoft_logs,
            analysis_results=analysis_payload,
        )
        if s3_result.get("success"):
            log.info(
                "Persisted One-Click artifacts to S3 bucket=%s prefix=%s keys=%s",
                s3_result.get("bucket"),
                s3_result.get("prefix"),
                s3_result.get("keys"),
            )
        else:
            log.error("Failed to persist S3 artifacts: %s", s3_result.get("error"))
    except Exception as e:
        log.error(f"Unexpected error persisting S3 artifacts: {e}")


if __name__ == "__main__":
    app.run()
