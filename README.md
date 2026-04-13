# Time Series Engine

This is a Time Series Engine for a crypto market data platform. Its primary role is to aggregate raw trade ticks into fixed-interval Open-High-Low-Close-Volume (OHLC) candles and expose them via REST APIs.

## Inner Workings & Data Flow

The engine essentially acts as a highly efficient middleman processing high-velocity tick data and persisting grouped analytics.

### 1. Where it takes data from (The Inputs)
There are two primary data sources functioning simultaneously to guarantee full coverage:
- **Real-Time Feed (Redis Stream)**:
  - Takes data from a Redis consumer group subscribed to the `market_trades` stream.
  - This provides lightning-fast streaming of recent events coming straight from exchanges (like Binance) in real-time.
- **Historical Feed (PostgreSQL Source)**:
  - Takes raw recorded ticks from a local Postgres database's `trades` table.
  - Used entirely as the "Source of Truth" to reliably construct missing candles in the past.

### 2. How it processes the data (The Engine)
- **Aggregation Strategy**: Whether the trades are streaming continuously from Redis or being extracted chunk-by-chunk from local Postgres, the core engine parses the current minute (or 5-minute) boundary of a trade's timestamp.
- **Forming Candles**: Inside memory dicts, it groups data. The very first price of a boundary sets `open`, while the maximum sets `high`, minimum sets `low`, current price sets `close`, and all quantities are rolled continuously into `volume`.
- **Upserting (Conflict Avoidance)**: Operations do not blind-insert but systematically `UPSERT` on combinations of `(symbol, timestamp)` index boundaries, automatically taking the maximum `high`, lowering `low`, and swapping `close` properly.

### 3. Where it sends data to (The Outputs)
- **Time-Series Warehouse (Neon DB)**:
  - The calculated OHLC candles are flushed directly into the free-tier cloud Neon server structure (Tables: `candles_1m` and `candles_5m`). This acts as our persistent long-term storage capable of servicing time-range API queries rapidly.

## Setup

1. Copy `.env.example` to `.env` and fill in the parameters.
   ```
   cp .env.example .env
   ```
   
2. Install requirements using `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```

3. Run the application:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

## REST API Endpoints

Once running, querying the finalized candles natively sits upon the FastAPI router.
- `GET /candles`: Fetch historical ranges. Requires `symbol`, `interval` (`1m` or `5m`), `start_time`, `end_time`. 
- `GET /latest-candle`: Query the most up-to-date compiled aggregation parameters in-flight.
- `GET /health`: Node status probing.