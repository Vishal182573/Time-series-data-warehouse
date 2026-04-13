import redis.asyncio as redis
from app.config import settings
from app.utils.logger import get_logger
import asyncio
from typing import Callable, Awaitable

logger = get_logger("RedisConsumer")

class RedisConsumer:
    def __init__(self):
        self.stream_name = "market_trades"
        self.group_name = "ts_engine_group"
        self.consumer_name = "ts_engine_worker_1"
        self.redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

    async def init_group(self):
        try:
            await self.redis.xgroup_create(self.stream_name, self.group_name, id="0", mkstream=True)
            logger.info(f"Consumer group {self.group_name} created.")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("Consumer group already exists.")
            else:
                raise e

    async def consume(self, callback: Callable[[list[dict]], Awaitable[None]]):
        await self.init_group()
        last_id = ">"
        
        while True:
            try:
                # Block for 2 seconds to fetch new entries
                messages = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: last_id},
                    count=100,
                    block=2000
                )
                
                if not messages:
                    continue
                    
                for stream, records in messages:
                    if not records:
                        continue
                    
                    parsed_trades = []
                    message_ids = []
                    for record_id, record in records:
                        parsed_trades.append({
                            "symbol": record["symbol"],
                            "price": float(record["price"]),
                            "quantity": float(record["quantity"]),
                            "timestamp": record["timestamp"], # It should be parsed to dt later
                            "exchange": record["exchange"],
                            "_redis_id": record_id
                        })
                        message_ids.append(record_id)
                        
                    await callback(parsed_trades)
                    
                    # Acknowledge messages
                    if message_ids:
                        await self.redis.xack(self.stream_name, self.group_name, *message_ids)
                        
            except Exception as e:
                logger.error(f"Error consuming from Redis: {e}")
                await asyncio.sleep(5)
