"""Query Datadog for Mulesoft logs using Correlation/Context IDs."""
from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)

async def query_mulesoft_logs(ctx_id: str, trigger_time: str, credentials: dict) -> dict:
    """Queries Datadog for Mulesoft logs matching the context/correlation ID."""
    if not ctx_id:
        return {"success": False, "error": "No context ID provided"}

    api_key = credentials['api_key']
    app_key = credentials['application_key']
    endpoint = credentials['endpoint']

    headers = {
        'DD-API-KEY': api_key,
        'DD-APPLICATION-KEY': app_key,
        'Content-Type': 'application/json'
    }

    # Define time window around the trigger time
    # Expand lookback and lookforward to handle timezone offsets and indexing delays
    try:
        trigger_dt = datetime.fromisoformat(trigger_time.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        trigger_dt = datetime.now()
        
    start_dt = trigger_dt - timedelta(hours=12)
    end_dt = trigger_dt + timedelta(hours=12) 
    
    # Query for the specific context ID across Mulesoft services
    query = f"service:mulesoft* (@correlation_id:*{ctx_id}* OR @ctx_id:*{ctx_id}* OR @correlationId:*{ctx_id}* OR \"{ctx_id}\")"

    logger.info(f"Querying Datadog for Mulesoft logs: ctx_id={ctx_id}")
    logger.info(f"Time window: {start_dt.isoformat()} to {end_dt.isoformat()}")
    logger.info(f"Query string: {query}")

    body = {
        'filter': {
            'from': start_dt.isoformat(),
            'to': end_dt.isoformat(),
            'query': query,
        },
        'sort': 'timestamp',
        'page': {'limit': 50},
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(endpoint, headers=headers, json=body, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            events = data.get('data', [])
            
            logger.info(f"Datadog returned {len(events)} events for Mulesoft correlation")
            
            logs = []
            for event in events:
                # Handle both internal and flattened attribute structures
                root_attrs = event.get("attributes", {})
                inner_attrs = root_attrs.get("attributes", {})
                
                logs.append({
                    "timestamp": root_attrs.get("timestamp"),
                    "message": root_attrs.get("message"),
                    "service": root_attrs.get("service"),
                    "correlation_id": inner_attrs.get("correlationId") or inner_attrs.get("correlation_id") or ctx_id
                })
                
            return {"success": True, "ctx_id": ctx_id, "logs": logs}
    except Exception as e:
        logger.error(f"Mulesoft log fetch failed: {e}")
        return {"success": False, "error": str(e)}
