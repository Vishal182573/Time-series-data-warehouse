from fastapi import WebSocket
from typing import List
import json
from app.utils.logger import get_logger

logger = get_logger("WSManager")

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("WebSocket client disconnected.")

    async def broadcast_candles(self, candles: list, interval: str):
        if not self.active_connections:
            return
            
        data = {
            "type": "candle_update",
            "interval": interval,
            "candles": [
                {
                    "symbol": c.symbol,
                    "timestamp": c.timestamp.isoformat(),
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "volume": float(c.volume)
                } for c in candles
            ]
        }
        
        message = json.dumps(data)
        
        # Broadcast safely to all connected clients
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.append(connection)
                
        for dead_conn in disconnected:
            self.disconnect(dead_conn)

manager = ConnectionManager()
