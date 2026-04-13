import psycopg
from psycopg_pool import AsyncConnectionPool
from contextlib import asynccontextmanager
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("DB")

neon_pool = None
source_pool = None

async def init_db():
    global neon_pool, source_pool
    # Initialize connection pools
    neon_pool = AsyncConnectionPool(settings.NEON_DB_URL, min_size=1, max_size=10)
    source_pool = AsyncConnectionPool(settings.POSTGRES_SOURCE_URL, min_size=1, max_size=5)
    
    # Ensure Neon DB has tables
    await _ensure_tables()

async def close_db():
    if neon_pool:
        await neon_pool.close()
    if source_pool:
        await source_pool.close()

async def _ensure_tables():
    async with neon_pool.connection() as conn:
        async with conn.cursor() as cur:
            tables = ["candles_1m", "candles_5m"]
            for table in tables:
                await cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    PRIMARY KEY (symbol, timestamp)
                )
                """)
                # Create index
                await cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol_ts ON {table} (symbol, timestamp)")
            await conn.commit()
    logger.info("Neon DB tables ensured.")

@asynccontextmanager
async def get_neon_conn():
    async with neon_pool.connection() as conn:
        yield conn

@asynccontextmanager
async def get_source_conn():
    async with source_pool.connection() as conn:
        yield conn
