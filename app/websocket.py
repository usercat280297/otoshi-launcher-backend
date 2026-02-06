from typing import Dict, Set

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str) -> None:
        await websocket.accept()
        self.active_connections.setdefault(user_id, set()).add(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str) -> None:
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def send_to_user(self, user_id: str, message: dict) -> None:
        for connection in self.active_connections.get(user_id, set()):
            await connection.send_json(message)

    async def broadcast(self, message: dict) -> None:
        for connections in self.active_connections.values():
            for connection in connections:
                await connection.send_json(message)


manager = ConnectionManager()
