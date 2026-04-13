from app.db.database import get_source_conn
from typing import AsyncGenerator
import psycopg
from app.utils.logger import get_logger

logger = get_logger("PostgresStorage")

class PostgresStorage:

    @staticmethod
    async def get_trades_batch(batch_size: int = 10000) -> AsyncGenerator[list, None]:
        # Connect to Postgres using a server-side cursor for fetching large datasets
        async with get_source_conn() as conn:
            # We'll fetch ordered by timestamp to simulate stream of trades
            async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                await cur.execute("SELECT symbol, price, quantity, timestamp, exchange FROM trades ORDER BY timestamp ASC")
                while True:
                    batch = await cur.fetchmany(batch_size)
                    if not batch:
                        break
                    yield batch
