from app.db.database import get_neon_conn
from typing import List, Tuple
from app.schemas.candle import CandleSchema
import psycopg
from app.utils.logger import get_logger

logger = get_logger("NeonStorage")

class NeonStorage:
    
    @staticmethod
    async def upsert_candles(candles: List[CandleSchema], interval: str):
        table_name = f"candles_{interval}"
        if not candles:
            return

        async with get_neon_conn() as conn:
            async with conn.cursor() as cur:
                # Use batch execution
                query = f"""
                INSERT INTO {table_name} (symbol, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO UPDATE 
                SET high = GREATEST({table_name}.high, EXCLUDED.high),
                    low = LEAST({table_name}.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = {table_name}.volume + EXCLUDED.volume
                """
                
                params = [
                    (c.symbol, c.timestamp, c.open, c.high, c.low, c.close, c.volume)
                    for c in candles
                ]
                await cur.executemany(query, params)
            await conn.commit()

    @staticmethod
    async def get_candles(symbol: str, interval: str, start_time, end_time) -> List[dict]:
        table_name = f"candles_{interval}"
        async with get_neon_conn() as conn:
            async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                await cur.execute(f"""
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM {table_name}
                WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
                """, (symbol, start_time, end_time))
                return await cur.fetchall()

    @staticmethod
    async def get_latest_candle(symbol: str, interval: str) -> dict:
        table_name = f"candles_{interval}"
        async with get_neon_conn() as conn:
            async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                await cur.execute(f"""
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM {table_name}
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
                """, (symbol,))
                row = await cur.fetchone()
                return row

    @staticmethod
    async def check_has_candles() -> bool:
        async with get_neon_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM candles_1m LIMIT 1")
                res = await cur.fetchone()
                return res is not None
