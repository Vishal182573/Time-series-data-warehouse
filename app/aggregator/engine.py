from app.schemas.candle import CandleSchema
from app.storage.neon_storage import NeonStorage
from app.utils.logger import get_logger
from datetime import datetime, timezone
from typing import Dict, Tuple

logger = get_logger("Aggregator")

class RealtimeAggregator:
    
    def __init__(self):
        self.candles_1m_map: Dict[Tuple[str, datetime], CandleSchema] = {}
        self.candles_5m_map: Dict[Tuple[str, datetime], CandleSchema] = {}

    def _truncate_time(self, dt: datetime, minutes: int) -> datetime:
        # Avoid naive/aware dt issues. Try to parse timezone string if it has one.
        delta = dt.minute % minutes
        return dt.replace(minute=dt.minute - delta, second=0, microsecond=0)

    async def process_trades(self, trades: list[dict]):
        if not trades:
            return

        self.candles_1m_map.clear()
        self.candles_5m_map.clear()
        
        for trade in trades:
            # timestamp could be string, parse it.
            ts_str = trade["timestamp"]
            # Fast parsing - e.g. "2026-03-21T11:43:35.123456+00:00"
            if isinstance(ts_str, str):
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            else:
                ts = ts_str # already dt
            
            trade["timestamp_dt"] = ts

            self._aggregate_candle(trade, self.candles_1m_map, 1)
            self._aggregate_candle(trade, self.candles_5m_map, 5)
            
        # Flush to DB immediately for real-time (in production one might use a batcher/timer)
        try:
            from app.api.ws_manager import manager
            
            c1 = list(self.candles_1m_map.values())
            c5 = list(self.candles_5m_map.values())
            
            if c1:
                await manager.broadcast_candles(c1, "1m") # Stream to active UIs FIRST
                await NeonStorage.upsert_candles(c1, "1m")
                
            if c5:
                await manager.broadcast_candles(c5, "5m") # Stream to active UIs FIRST
                await NeonStorage.upsert_candles(c5, "5m")
                
            logger.debug(f"Aggregated and upserted {len(c1)} 1m candles and {len(c5)} 5m candles.")
        except Exception as e:
            logger.error(f"Error upserting aggregated candles: {e}")

    def _aggregate_candle(self, trade: dict, map_dict: dict, interval_minutes: int):
        symbol = trade["symbol"]
        dt = trade["timestamp_dt"]
        price = trade["price"]
        qty = trade["quantity"]
        
        period_start = self._truncate_time(dt, interval_minutes)
        key = (symbol, period_start)
        
        if key not in map_dict:
            map_dict[key] = CandleSchema(
                symbol=symbol,
                timestamp=period_start,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=qty
            )
        else:
            candle = map_dict[key]
            candle.high = max(candle.high, price)
            candle.low = min(candle.low, price)
            # Since Redis streams events in-order, the last one processed in the batch is latest
            candle.close = price
            candle.volume += qty
