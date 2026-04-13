import redis
import json
from datetime import datetime
import asyncio
from app.storage.neon_storage import NeonStorage
from app.schemas.candle import CandleSchema
from app.db.database import get_neon_conn, neon_pool, init_db

async def run_test():
    await init_db()
    r = redis.Redis.from_url('redis://default:62Dr8twH5SAjrHNzlGw8o2YT3V5eeG90@redis-18263.c8.us-east-1-4.ec2.cloud.redislabs.com:18263', decode_responses=True)
    data = r.xrevrange('market_trades', count=1)[0][1]
    
    ts_str = data["timestamp"]
    if isinstance(ts_str, str):
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        
    c = CandleSchema(
        symbol=data["symbol"],
        timestamp=ts,
        open=float(data["price"]),
        high=float(data["price"]),
        low=float(data["price"]),
        close=float(data["price"]),
        volume=float(data["quantity"])
    )
    
    try:
        await NeonStorage.upsert_candles([c], "1m")
        print("Upsert succeeded!")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Upsert failed:", repr(e))

asyncio.run(run_test())
