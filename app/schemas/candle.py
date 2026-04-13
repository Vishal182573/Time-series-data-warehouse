from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class TradeSchema(BaseModel):
    symbol: str
    price: float
    quantity: float
    timestamp: datetime
    exchange: Optional[str] = "binance"
    _redis_id: Optional[str] = None

class CandleSchema(BaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
