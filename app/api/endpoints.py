from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from app.storage.neon_storage import NeonStorage
from app.api.ws_manager import manager
from typing import List, Optional
from datetime import datetime

router = APIRouter()

@router.websocket("/ws/candles")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We just keep the connection open waiting for client disconnects
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@router.get("/candles", response_model=List[dict])
async def get_candles(
    symbol: str, 
    interval: str = Query(..., regex="^(1m|5m)$"), 
    start_time: datetime = Query(...), 
    end_time: datetime = Query(...)
):
    try:
        candles = await NeonStorage.get_candles(symbol, interval, start_time, end_time)
        return candles
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def get_health():
    return {"status": "healthy"}

@router.get("/latest-candle")
async def get_latest_candle(
    symbol: str, 
    interval: str = Query(..., regex="^(1m|5m)$")
):
    try:
        candle = await NeonStorage.get_latest_candle(symbol, interval)
        if not candle:
            raise HTTPException(status_code=404, detail="Candle not found")
        return candle
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
