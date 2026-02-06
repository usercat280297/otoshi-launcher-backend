"""WebSocket handler for live update notifications."""

import logging
import json
from typing import Set
from fastapi import WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class UpdateConnectionManager:
    """Manages WebSocket connections for live update broadcasting."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        """Register new client connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove client connection."""
        self.active_connections.discard(websocket)
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast_live_edit(self, file_path: str, content_hash: str) -> None:
        """Broadcast live edit to all connected clients.

        Args:
            file_path: Path of edited file (e.g., 'assets/ui/main.json')
            content_hash: SHA256 hash of new content
        """
        message = json.dumps({
            "type": "live-edit",
            "filePath": file_path,
            "contentHash": content_hash,
        })

        disconnected = []
        for websocket in self.active_connections:
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected.append(websocket)

        # Clean up disconnected clients
        for ws in disconnected:
            await self.disconnect(ws)

        logger.info(f"Broadcasted live edit to {len(self.active_connections)} clients")

    async def broadcast_update_available(
        self,
        version: str,
        changelog: str,
        force_update: bool = False
    ) -> None:
        """Broadcast update notification to all connected clients.

        Args:
            version: New version number
            changelog: Update changelog
            force_update: Whether to force immediate update
        """
        message = json.dumps({
            "type": "update-available",
            "version": version,
            "changelog": changelog,
            "forceUpdate": force_update,
        })

        disconnected = []
        for websocket in self.active_connections:
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected.append(websocket)

        # Clean up disconnected clients
        for ws in disconnected:
            await self.disconnect(ws)

        logger.info(f"Broadcasted update notification to {len(self.active_connections)} clients")

    async def broadcast_rollback(self, target_version: str) -> None:
        """Broadcast rollback notification to all connected clients."""
        message = json.dumps({
            "type": "rollback",
            "targetVersion": target_version,
        })

        disconnected = []
        for websocket in self.active_connections:
            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected.append(websocket)

        # Clean up disconnected clients
        for ws in disconnected:
            await self.disconnect(ws)

        logger.info(f"Broadcasted rollback to {len(self.active_connections)} clients")

    def get_connection_count(self) -> int:
        """Get number of active connections."""
        return len(self.active_connections)


# Global instance
manager = UpdateConnectionManager()


async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint for live updates.

    Usage from frontend:
        const ws = new WebSocket('ws://localhost:8000/ws/updates');
        ws.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (message.type === 'live-edit') {
                // Reload file
            }
        };
    """
    await manager.connect(websocket)

    try:
        while True:
            # Keep connection open and wait for messages
            # In practice, we only send, don't receive from client
            data = await websocket.receive_text()
            # Optional: handle ping/keepalive messages
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await manager.disconnect(websocket)
