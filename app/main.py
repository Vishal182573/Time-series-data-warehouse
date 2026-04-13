import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.db.database import init_db, close_db
from app.backfill.engine import BackfillEngine
from app.consumer.redis_client import RedisConsumer
from app.aggregator.engine import RealtimeAggregator
from app.api.endpoints import router as api_router
from app.utils.logger import get_logger

logger = get_logger("Main")

redis_consumer: RedisConsumer = None
realtime_aggregator: RealtimeAggregator = None
consumer_task = None

async def start_consumer():
    global redis_consumer, realtime_aggregator
    redis_consumer = RedisConsumer()
    realtime_aggregator = RealtimeAggregator()

    # Pass aggregator process function as callback to consumer
    await redis_consumer.init_group()
    
    # Process trades callback wrapper
    async def handle_messages(trades):
        await realtime_aggregator.process_trades(trades)
        
    await redis_consumer.consume(handle_messages)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    # 1. Connect to Neon DB & Postgres Source
    logger.info("Initializing databases...")
    await init_db()

    # 2 & 3. Run backfill from PostgreSQL if no candles exist
    logger.info("Starting Backfill Engine...")
    backfill_engine = BackfillEngine()
    await backfill_engine.run()

    # 4 & 5. Start Redis consumer and real-time aggregation as a background task
    logger.info("Starting real-time consumer...")
    consumer_task = asyncio.create_task(start_consumer())

    yield

    # Shutdown logic
    logger.info("Shutting down...")
    if consumer_task:
        consumer_task.cancel()
    await close_db()


app = FastAPI(title="Time Series Engine", lifespan=lifespan)

# Mount endpoints
app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
