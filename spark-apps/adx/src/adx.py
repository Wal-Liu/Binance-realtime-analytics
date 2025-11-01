from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, max as _max, min as _min, first, last, abs as _abs, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import psycopg2
import json
from pathlib import Path


# === Load Configs ===
KAFKA_FILE = Path("/opt/workspace/adx/configs/kafka.json")
POSTGRE_FILE = Path("/opt/workspace/adx/configs/postgres.json")

with open(KAFKA_FILE, "r", encoding="utf-8") as f:
    KAFKA = json.load(f)

with open(POSTGRE_FILE, "r", encoding="utf-8") as f:
    POSTGRE = json.load(f)


# === PostgreSQL helpers ===
def create_table():
    """Tạo bảng lưu ADX nếu chưa tồn tại"""
    conn = psycopg2.connect(
        host=POSTGRE["host"].split(":")[0],
        port=int(POSTGRE["host"].split(":")[1]),
        database=POSTGRE["db"],
        user=POSTGRE["user"],
        password=POSTGRE["password"]
    )
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS crypto_adx (
        id SERIAL PRIMARY KEY,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        symbol VARCHAR(20),
        adx DOUBLE PRECISION,
        plus_di DOUBLE PRECISION,
        minus_di DOUBLE PRECISION,
        UNIQUE (window_start, window_end, symbol)
    );
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_adx_window_start ON crypto_adx(window_start);")
    conn.commit()
    cursor.close()
    conn.close()


def write_to_postgres(batch_df, batch_id):
    """Ghi batch vào PostgreSQL"""
    if batch_df.rdd.isEmpty():
        return

    create_table()
    print("Đang insert")
    conn = psycopg2.connect(
        host=POSTGRE["host"].split(":")[0],
        port=int(POSTGRE["host"].split(":")[1]),
        database=POSTGRE["db"],
        user=POSTGRE["user"],
        password=POSTGRE["password"]
    )
    cursor = conn.cursor()

    for row in batch_df.collect():
        cursor.execute("""
            INSERT INTO crypto_adx (window_start, window_end, symbol, adx, plus_di, minus_di)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (window_start, window_end, symbol)
            DO UPDATE SET 
                adx = EXCLUDED.adx,
                plus_di = EXCLUDED.plus_di,
                minus_di = EXCLUDED.minus_di;
        """, (row.window_start, row.window_end, row.symbol, row.adx, row.plus_di, row.minus_di))

    conn.commit()
    cursor.close()
    conn.close()


# === Spark session ===
spark = (
    SparkSession.builder
    .appName("CryptoADXStreamV1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# === Schema của dữ liệu Kline từ Kafka ===
schema = StructType([
    StructField("symbol", StringType()),
    StructField("interval", StringType()),
    StructField("open_time", LongType()),
    StructField("close_time", LongType()),
    StructField("open_price", DoubleType()),
    StructField("high_price", DoubleType()),
    StructField("low_price", DoubleType()),
    StructField("close_price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("quote_volume", DoubleType()),
    StructField("number_of_trades", LongType()),
    StructField("is_closed", BooleanType()),
    StructField("taker_buy_volume", DoubleType()),
    StructField("taker_buy_quote_volume", DoubleType()),
    StructField("event_time", LongType())
])



# === Đọc stream từ Kafka ===
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA["bootstrap.servers"])
    .option("subscribe", KAFKA["kline_topic"])   # 🔹 dùng đúng key "kline_topic"
    .option("startingOffsets", "latest")
    .load()
)

# === Parse JSON từ Kafka ===
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

# === Chuẩn hoá thời gian ===
df_with_time = df_parsed.withColumn("event_time", (col("event_time") / 1000).cast("timestamp"))
df_parsed.select("symbol", "high_price", "low_price", "close_price", "event_time") \
    .writeStream.format("console").outputMode("append").start()
# === Tính ADX (Aggregation-based, không dùng lag) ===
# Tính toán trong cửa sổ 14 phút, trượt 1 phút

adx_df = (
    df_with_time
    .withWatermark("event_time", "2 minutes")
    .groupBy(window(col("event_time"), "14 minutes", "1 minute"), col("symbol"))
    .agg(
        first("high_price").alias("first_high"),
        last("high_price").alias("last_high"),
        first("low_price").alias("first_low"),
        last("low_price").alias("last_low"),
        first("close_price").alias("first_close"),
        last("close_price").alias("last_close"),
        _max("high_price").alias("max_high"),
        _min("low_price").alias("min_low")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "symbol",
        # True Range (đơn giản hóa)
        expr("max_high - min_low").alias("tr_range"),
        # Directional Movement (đơn giản hóa)
        expr("abs(last_high - first_high)").alias("plus_dm"),
        expr("abs(last_low - first_low)").alias("minus_dm"),
        # DI+
        expr("100 * (abs(last_high - first_high) / (max_high - min_low + 0.00001))").alias("plus_di"),
        # DI-
        expr("100 * (abs(last_low - first_low) / (max_high - min_low + 0.00001))").alias("minus_di"),
        # ADX tạm (đơn giản hóa)
        expr("100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 0.00001))").alias("adx")
    )
)

# === Ghi stream vào PostgreSQL ===
create_table()

query = (
    adx_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/opt/workspace/adx/checkpoints/adx_indicator_v1")
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
