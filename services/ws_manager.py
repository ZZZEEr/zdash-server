"""
WebSocket connection manager with enhanced stability.
"""

import asyncio
import json
import logging
from typing import Any, Dict, Set
from collections import deque

from fastapi import WebSocket

logger = logging.getLogger("zdash.ws_manager")

# Message queue settings
MAX_QUEUE_SIZE = 1000
MESSAGE_TIMEOUT = 5.0


class WSManager:
    """Manages WebSocket connections with message queuing and error handling."""

    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._message_queue: deque = deque(maxlen=MAX_QUEUE_SIZE)

    async def connect(self, ws: WebSocket) -> None:
        """Accept and register a WebSocket connection."""
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)
        logger.info(f"WebSocket connected, total: {len(self._connections)}")

    async def disconnect(self, ws: WebSocket) -> None:
        """Unregister a WebSocket connection."""
        async with self._lock:
            self._connections.discard(ws)
        logger.info(f"WebSocket disconnected, total: {len(self._connections)}")

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """Broadcast message to all connected clients with error handling."""
        async with self._lock:
            if not self._connections:
                return

            dead: Set[WebSocket] = set()
            payload = json.dumps(message)

            # Store in queue for potential replay
            self._message_queue.append(message)

            for ws in self._connections:
                try:
                    await asyncio.wait_for(
                        ws.send_text(payload),
                        timeout=MESSAGE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"WebSocket send timeout, marking for removal")
                    dead.add(ws)
                except Exception as e:
                    logger.debug(f"WebSocket send error: {e}")
                    dead.add(ws)

            # Remove dead connections
            for ws in dead:
                self._connections.discard(ws)
                try:
                    await ws.close()
                except Exception:
                    pass

    async def broadcast_node_update(self, node_list_data: Dict[str, Any]) -> None:
        """Broadcast node update event."""
        await self.broadcast({
            "type": "node_update",
            "data": node_list_data,
            "timestamp": asyncio.get_event_loop().time(),
        })

    async def broadcast_alert(self, alert_data: Dict[str, Any]) -> None:
        """Broadcast alert event."""
        await self.broadcast({
            "type": "alert",
            "data": alert_data,
            "timestamp": asyncio.get_event_loop().time(),
        })

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    def get_recent_messages(self, limit: int = 10) -> list:
        """Get recent messages from queue (for debugging)."""
        return list(self._message_queue)[-limit:]


ws_manager = WSManager()