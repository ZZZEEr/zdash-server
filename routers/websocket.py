"""WS /api/v1/ws — Real-time WebSocket push endpoint."""

import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from server.services.ws_manager import WSManager

logger = logging.getLogger("zdash.ws")

router = APIRouter()


@router.websocket("/api/v1/ws")
async def websocket_endpoint(ws: WebSocket):
    manager: WSManager = ws.app.state.ws_manager

    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(ws)
