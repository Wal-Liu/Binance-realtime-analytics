import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from typing import Iterator
import pandas as pd

# ---- CẤU HÌNH ----
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "binance_kline_streams"
SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "BollingerBands_Streaming_App"

PG_URL = "jdbc:postgresql://postgres:5432/crypto_db"
PG_TABLE = "bollinger_bands_1m"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "your_password",
    "driver": "org.postgresql.Driver",
}

BB_PERIOD = 20
BB_STDDEV = 2
STATE_TIMEOUT_MS = 30 * 24 * 60 * 60 * 1000  # 30 ngày

# ---- SCHEMA ----
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

# ---- HÀM TÍNH BB ----
def calculate_bb(
    symbol: str,
    inputs: Iterator[pd.DataFrame],
    state: "GroupState"
) -> Iterator[pd.DataFrame]:
    # LẤY STATE CŨ: Row(prices=[...])
    prices = state.get().prices if state.exists else []  # SỬA: .prices

    # Gộp batch + sort theo timestamp
    batch_df = pd.concat(list(inputs), axis=0)
    batch_df = batch_df.sort_values("timestamp").reset_index(drop=True)

    results = []
    for _, row in batch_df.iterrows():
        price = row["close_price"]
        ts = row["timestamp"]

        # Cập nhật danh sách giá
        prices.append(price)
        if len(prices) > BB_PERIOD:
            prices.pop(0)

        # Tính BB khi đủ 20 kỳ
        if len(prices) >= BB_PERIOD:
            sma = sum(prices) / len(prices)
            variance = sum((x - sma) ** 2 for x in prices) / len(prices)
            stddev = variance ** 0.5
            results.append({
                "symbol": symbol,
                "timestamp": ts,
                "close_price": price,
                "sma": sma,
                "stddev": stddev,
                "upper_band": sma + BB_STDDEV * stddev,
                "lower_band": sma - BB_STDDEV * stddev
            })

    # Cập nhật state: Row(prices=prices)
    state.update({"prices": prices})
    state.setTimeoutDuration(STATE_TIMEOUT_MS)

    return iter([pd.DataFrame(results)] if results else [])

# ---- GHI POSTGRES ----
def write_to_postgres(df, epoch_id):
    print(f"--- Ghi Epoch ID: {epoch_id} ---")
    if df.rdd.isEmpty():
        print("Không có dữ liệu mới.")
        return

    output_df = df.select(
        "symbol", "timestamp", "close_price",
        "sma", "stddev", "upper_band", "lower_band"
    ).orderBy("timestamp")

    output_df.show(5, truncate=False)
    output_df.write.jdbc(url=PG_URL, table=PG_TABLE, mode="append", properties=PG_PROPERTIES)

# ---- MAIN ----
def main():
    print("Khởi tạo Spark Session...")

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.6.0",
    ]

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session đã sẵn sàng.")

    # 1. Đọc Kafka
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON
    parsed_df = kafka_df.select(F.col("value").cast("string").alias("json_value")) \
        .select(F.from_json("json_value", kline_schema).alias("data")) \
        .select("data.*")

    # 3. Xử lý
    processed_df = parsed_df.filter(F.col("is_closed") == True) \
        .withColumn("timestamp", (F.col("close_time") / 1000).cast(TimestampType())) \
        .select("symbol", "close_price", "timestamp")

    # 4. TÍNH BB DÙNG applyInPandasWithState
    bb_stream = processed_df \
        .groupBy("symbol") \
        .applyInPandasWithState(
            func=calculate_bb,
            outputStructType=StructType([
                StructField("symbol", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("close_price", DoubleType(), False),
                StructField("sma", DoubleType(), False),
                StructField("stddev", DoubleType(), False),
                StructField("upper_band", DoubleType(), False),
                StructField("lower_band", DoubleType(), False),
            ]),
            stateStructType=StructType([
                StructField("prices", ArrayType(DoubleType()), False)
            ]),
            outputMode="append",
            timeoutConf="ProcessingTimeTimeout"
        )

    # 5. Ghi
    query = bb_stream.writeStream \
        .foreachBatch(write_to_postgres) \
        .trigger(processingTime="1 minute") \
        .start()

    print(f"Đang lắng nghe {KAFKA_TOPIC} và tính Bollinger Bands...")
    query.awaitTermination()

if __name__ == "__main__":
    main()