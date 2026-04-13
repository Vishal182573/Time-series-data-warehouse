from app.storage.postgres_storage import PostgresStorage
from app.storage.neon_storage import NeonStorage
from app.schemas.candle import CandleSchema
from app.utils.logger import get_logger
import asyncio
from typing import Dict
from datetime import datetime, timedelta

logger = get_logger("BackfillEngine")

class BackfillEngine:
    
    def __init__(self):
        self.candles_1m_map: Dict[Tuple[str, datetime], CandleSchema] = {}
        self.candles_5m_map: Dict[Tuple[str, datetime], CandleSchema] = {}

    def _truncate_time(self, dt: datetime, minutes: int) -> datetime:
        # e.g., 5m candle truncates 12:34 to 12:30
        delta = dt.minute % minutes
        return dt.replace(minute=dt.minute - delta, second=0, microsecond=0)

    def _process_trade(self, trade_dict: dict, map_dict: dict, interval_minutes: int):
        symbol = trade_dict["symbol"]
        dt = trade_dict["timestamp"]
        price = float(trade_dict["price"])
        qty = float(trade_dict["quantity"])
        
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
            candle.close = price
            candle.volume += qty

    async def run(self):
        logger.info("Starting backfill from Postgres to Neon...")
        try:
            has_candles = await NeonStorage.check_has_candles()
            if has_candles:
                logger.info("Candles already exist in Neon DB. Skipping backfill.")
                return
            
            total_trades = 0
            async for batch in PostgresStorage.get_trades_batch():
                self.candles_1m_map.clear()
                self.candles_5m_map.clear()
                
                for trade in batch:
                    self._process_trade(trade, self.candles_1m_map, 1)
                    self._process_trade(trade, self.candles_5m_map, 5)
                    
                total_trades += len(batch)
                
                # Write to Neon
                await NeonStorage.upsert_candles(list(self.candles_1m_map.values()), "1m")
                await NeonStorage.upsert_candles(list(self.candles_5m_map.values()), "5m")
                
                logger.info(f"Processed batch of size {len(batch)}. Total so far: {total_trades}")
            
            logger.info("Backfill completed successfully.")
        except Exception as e:
            logger.error(f"Backfill engine failed with error: {e}")
