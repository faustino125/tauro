"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Real-time log streaming via WebSocket
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from typing import Optional
from loguru import logger
import asyncio
from datetime import datetime

from tauro.api.core.deps import get_run_service

router = APIRouter(prefix="/logs", tags=["logs"])


# =============================================================================
# WebSocket - Stream logs in real-time
# =============================================================================


@router.websocket("/runs/{run_id}/stream")
async def stream_logs(
    websocket: WebSocket,
    run_id: str,
    level: Optional[str] = Query(None, description="Filter by log level"),
):
    """
    Stream logs in real-time via WebSocket.
    
    Connect to this endpoint to receive log messages as they're generated.
    Useful for live monitoring of pipeline execution.
    
    Example client usage (JavaScript):
    ```javascript
    const ws = new WebSocket('ws://localhost:8000/api/v1/logs/runs/{run_id}/stream');
    ws.onmessage = (event) => {
        const log = JSON.parse(event.data);
        console.log(log.message);
    };
    ```
    """
    await websocket.accept()
    logger.info(f"WebSocket connected for run {run_id}")
    
    try:
        # Get run service (would need dependency injection in real implementation)
        last_timestamp = None
        
        while True:
            # Poll for new logs every 500ms
            # In production, consider using pub/sub pattern instead
            await asyncio.sleep(0.5)
            
            # Fetch new logs since last timestamp
            # This is a simplified implementation
            # Real implementation should use async streaming from store
            try:
                # TODO: Implement actual log streaming from store
                # For now, send a heartbeat
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": datetime.utcnow().isoformat()
                })
            except Exception as e:
                logger.error(f"Error fetching logs: {e}")
                break
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for run {run_id}")
    except Exception as e:
        logger.error(f"WebSocket error for run {run_id}: {e}")
        await websocket.close(code=1011, reason=str(e))


# =============================================================================
# Alternative: Server-Sent Events (SSE) - Simpler than WebSocket
# =============================================================================


@router.get("/runs/{run_id}/stream-sse")
async def stream_logs_sse(
    run_id: str,
    level: Optional[str] = Query(None, description="Filter by log level"),
    run_service=Depends(get_run_service),
):
    """
    Stream logs using Server-Sent Events (SSE).
    
    Alternative to WebSocket, simpler and works over HTTP.
    Browser's EventSource API makes this trivial to consume.
    
    Example client usage (JavaScript):
    ```javascript
    const eventSource = new EventSource('/api/v1/logs/runs/{run_id}/stream-sse');
    eventSource.onmessage = (event) => {
        const log = JSON.parse(event.data);
        console.log(log.message);
    };
    ```
    """
    from fastapi.responses import StreamingResponse
    
    async def event_generator():
        """Generate SSE events"""
        try:
            last_index = 0
            
            while True:
                # Check if run is still active
                run = await run_service.get_run(run_id)
                if not run:
                    yield f"data: {{'error': 'Run not found'}}\n\n"
                    break
                
                # Fetch new logs
                logs, total = await run_service.get_run_logs(
                    run_id,
                    skip=last_index,
                    limit=100,
                    level=level,
                )
                
                # Send new logs
                for log in logs:
                    yield f"data: {log}\n\n"
                    last_index += 1
                
                # Check if run is finished
                if run.get("state") in ["COMPLETED", "FAILED", "CANCELLED"]:
                    yield f"data: {{'type': 'complete', 'state': '{run.get('state')}'}}\n\n"
                    break
                
                # Wait before next poll
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error streaming logs for run {run_id}: {e}")
            yield f"data: {{'error': '{str(e)}'}}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


__all__ = ["router"]
