import json
import time
from collections import deque, defaultdict
import psycopg2
import psycopg2.extras

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

# Config
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_SOURCE_TOPIC = "binance_kline_streams"

SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "Donchian_Channel_Streaming"

DONCHIAN_PERIOD = 20
PG_URL = "jdbc:postgresql://postgres:5432/crypto_db"
PG_TABLE = "donchian_channel_tbl"
PG_USER = "postgres"
PG_PASSWORD = "your_password"

PG_CONN_PARAMS = {
    "host": "postgres",
    "port": 5432,
    "dbname": "crypto_db",
    "user": PG_USER,
    "password": PG_PASSWORD,
}

CHECKPOINT_LOCATION = "/tmp/spark/donchian_checkpoints"

#Schema
kline_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("open_time", LongType(), True),
    StructField("close_time", LongType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("number_of_trades", LongType(), True),
    StructField("is_closed", BooleanType(), True),
    StructField("taker_buy_volume", DoubleType(), True),
    StructField("taker_buy_quote_volume", DoubleType(), True),
    StructField("event_time", LongType(), True),
])

#Postgres upsert & load tail state
def pg_connect():
    return psycopg2.connect(**PG_CONN_PARAMS)

def upsert_rows(conn, rows):
    """
    rows: list of dict matching table columns: symbol,timestamp,close_price,upper_band,lower_band,middle_band,breakout_signal,resolution
    Uses ON CONFLICT to upsert (requires unique index/PK defined).
    """
    if not rows:
        return
    cols = ["symbol","timestamp","close_price","upper_band","lower_band","middle_band","breakout_signal","resolution","created_at"]
    placeholders = ", ".join(["%s"] * len(cols))
    update_assign = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in ("symbol","timestamp","resolution")])
    sql = f"""
    INSERT INTO {PG_TABLE} ({", ".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT (symbol, timestamp, resolution) DO UPDATE SET
      {update_assign}
    ;
    """
    with conn.cursor() as cur:
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        args = []
        for r in rows:
            args.append((
                r["symbol"],
                r["timestamp"],
                r["close_price"],
                r["upper_band"],
                r["lower_band"],
                r["middle_band"],
                r["breakout_signal"],
                r.get("resolution", "tick"),
                now
            ))
        psycopg2.extras.execute_batch(cur, sql, args, page_size=500)
    conn.commit()

def load_last_n_per_symbol(conn, period):
    """
    Load last (period-1) highs and lows per symbol from PG to continue rolling.
    Returns dict: {symbol: list of (timestamp, high, low, close)} sorted by timestamp asc (oldest->newest)
    """
    out = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # choose resolution 'tick' stored by our job
        cur.execute(f"""
            SELECT symbol, timestamp, upper_band, lower_band, close_price
            FROM {PG_TABLE}
            WHERE resolution = 'tick'
            ORDER BY timestamp DESC
            LIMIT 10000; -- fetch reasonably many rows across symbols
        """)
        rows = cur.fetchall()
    # aggregate per symbol and keep newest (period-1)
    tmp = defaultdict(list)
    for r in rows:
        tmp[r["symbol"]].append((r["timestamp"], r["upper_band"], r["lower_band"], r["close_price"]))
    for s, rr in tmp.items():
        rr_sorted = sorted(rr, key=lambda x: x[0])
        out[s] = rr_sorted[-(period-1):] if len(rr_sorted) >= (period-1) else rr_sorted
    return out

# Processing by foreachBatch
def process_batch_and_upsert(batch_df, epoch_id):
    # Called each micro-batch with a Spark DataFrame (batch_df)
    if batch_df.rdd.isEmpty():
        print(f"Epoch {epoch_id}: empty batch")
        return

    print(f"Epoch {epoch_id}: start processing batch with {batch_df.count()} rows")

    # 1) prepare: convert timestamps, filter closed klines
    df = (
        batch_df
        .filter(F.col("is_closed") == True)
        .withColumn("timestamp", (F.col("close_time") / 1000).cast("timestamp"))
        .select("symbol", "timestamp", "high_price", "low_price", "close_price")
    )

    # 2) persist small staging df to speed repeated ops in this batch
    df = df.cache()

    # 3) Load external state (last DONCHIAN_PERIOD-1 highs/lows per symbol) from Postgres
    conn = pg_connect()
    try:
        tail_state = load_last_n_per_symbol(conn, DONCHIAN_PERIOD)
    finally:
        conn.close()

    # 4) For each symbol in this batch, collect, compute donchian
    symbols = [row.symbol for row in df.select("symbol").distinct().collect()]
    results = []  # list of dict rows for upsert
    for sym in symbols:
        sym_df = df.filter(F.col("symbol") == sym).orderBy("timestamp")
        #distributed stateful function
        rows = sym_df.select("timestamp", "high_price", "low_price", "close_price").collect()
        # reconstruct a deque of previous highs/lows
        prev = deque(maxlen=DONCHIAN_PERIOD-1)
        if sym in tail_state:
            for t, ub, lb, close in tail_state[sym]:
                prev.append((ub, lb)) 
        highs = []
        lows = []
        for r in rows:
            highs.append(r.high_price)
            lows.append(r.low_price)
            # Compute window bounds for the last DONCHIAN_PERIOD values
            win_high = max(highs[-DONCHIAN_PERIOD:]) if len(highs) >= 1 else None
            win_low = min(lows[-DONCHIAN_PERIOD:]) if len(lows) >= 1 else None

            prev_window_high = max(highs[-DONCHIAN_PERIOD:-1]) if len(highs) > 1 else None
            prev_window_low = min(lows[-DONCHIAN_PERIOD:-1]) if len(lows) > 1 else None

            breakout = "HOLD"
            if prev_window_high is not None and r.close_price > prev_window_high:
                breakout = "BUY"
            elif prev_window_low is not None and r.close_price < prev_window_low:
                breakout = "SELL"

            res = {
                "symbol": sym,
                "timestamp": r.timestamp,
                "close_price": r.close_price,
                "upper_band": float(win_high) if win_high is not None else None,
                "lower_band": float(win_low) if win_low is not None else None,
                "middle_band": (float(win_high) + float(win_low)) / 2 if (win_high is not None and win_low is not None) else None,
                "breakout_signal": breakout,
                "resolution": "tick",
            }
            results.append(res)

    # 5) Upsert results into Postgres
    if results:
        conn = pg_connect()
        try:
            upsert_rows(conn, results)
            print(f"Epoch {epoch_id}: Upserted {len(results)} rows to Postgres")
        finally:
            conn.close()
    else:
        print(f"Epoch {epoch_id}: No results to upsert")

    # 6) Multi-resolution aggregation: compute 1m/5m/15m rollups and upsert too
    for res_minutes in (1, 5, 15):
        windowed = (
            df
            .withWatermark("timestamp", "2 minutes")
            .groupBy("symbol", F.window("timestamp", f"{res_minutes} minutes"))
            .agg(
                F.max("high_price").alias("upper_band"),
                F.min("low_price").alias("lower_band"),
                F.last("close_price").alias("close_price")
            )
            .select(
                F.col("symbol"),
                F.col("window.end").alias("timestamp"),
                "close_price","upper_band","lower_band"
            )
        )
        # collect windowed and upsert
        rows = []
        for row in windowed.collect():
            if row.upper_band is None or row.lower_band is None:
                continue
            mband = (row.upper_band + row.lower_band) / 2.0
            rows.append({
                "symbol": row.symbol,
                "timestamp": row.timestamp,
                "close_price": row.close_price,
                "upper_band": float(row.upper_band),
                "lower_band": float(row.lower_band),
                "middle_band": float(mband),
                "breakout_signal": "HOLD",
                "resolution": f"{res_minutes}m",
            })
        if rows:
            conn = pg_connect()
            try:
                upsert_rows(conn, rows)
                print(f"Epoch {epoch_id}: Upserted {len(rows)} rows for {res_minutes}m")
            finally:
                conn.close()

    df.unpersist()

def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.adaptive.enabled", "true")   # AQE
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "2")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session ready.")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), kline_schema).alias("data")
    ).select("data.*")

    parsed_df = parsed_df.withColumn("timestamp", (F.col("close_time") / 1000).cast("timestamp"))
    parsed_df = parsed_df.withWatermark("timestamp", "2 minutes")

    # start streaming with foreachBatch for external-state upserts
    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch_and_upsert)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print(f"Streaming Donchian Channel (N={DONCHIAN_PERIOD}) from `{KAFKA_SOURCE_TOPIC}` → PostgreSQL `{PG_TABLE}` ...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
