from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, max as _max, min as _min, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
import psycopg2
import json
from pathlib import Path


# === Load Configs ===
KAFKA_FILE = Path("/opt/workspace/aroon/configs/kafka.json")
POSTGRE_FILE = Path("/opt/workspace/aroon/configs/postgres.json")

with open(KAFKA_FILE, "r", encoding="utf-8") as f:
    KAFKA = json.load(f)

with open(POSTGRE_FILE, "r", encoding="utf-8") as f:
    POSTGRE = json.load(f)


# === PostgreSQL helpers ===
def create_table():
    """Tạo bảng nếu chưa tồn tại"""
    conn = psycopg2.connect(
        host=POSTGRE["host"].split(":")[0],
        port=int(POSTGRE["host"].split(":")[1]),
        database=POSTGRE["db"],
        user=POSTGRE["user"],
        password=POSTGRE["password"]
    )
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS crypto_aroon (
        id SERIAL PRIMARY KEY,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        symbol VARCHAR(20),
        aroon_up DOUBLE PRECISION,
        aroon_down DOUBLE PRECISION,
        UNIQUE (window_start, window_end, symbol)
    );
    """
    cursor.execute(create_table_query)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_aroon_window_start ON crypto_aroon(window_start);")
    conn.commit()
    cursor.close()
    conn.close()


def write_to_postgres(batch_df, batch_id):
    """Ghi batch vào PostgreSQL"""
    if batch_df.rdd.isEmpty():
        return

    create_table()

    conn = psycopg2.connect(
        host=POSTGRE["host"].split(":")[0],
        port=int(POSTGRE["host"].split(":")[1]),
        database=POSTGRE["db"],
        user=POSTGRE["user"],
        password=POSTGRE["password"]
    )
    cursor = conn.cursor()
    for row in batch_df.collect():
        cursor.execute(
            """
            INSERT INTO crypto_aroon (window_start, window_end, symbol, aroon_up, aroon_down)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (window_start, window_end, symbol)
            DO UPDATE SET 
                aroon_up = EXCLUDED.aroon_up,
                aroon_down = EXCLUDED.aroon_down;
            """,
            (row.window_start, row.window_end, row.symbol, row.aroon_up, row.aroon_down)
        )
    conn.commit()
    cursor.close()
    conn.close()
    print("Insert thành công")


# === Spark session ===
spark = (
    SparkSession.builder
    .appName("CryptoAroonStream")
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
    StructField("event_time", LongType())
])


# === Đọc stream từ Kafka ===
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA["bootstrap.servers"])
    .option("subscribe", KAFKA["topic"])
    .option("startingOffsets", "latest")
    .load()
)

# === Parse JSON từ Kafka ===
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# === Thêm cột timestamp chuẩn cho Spark ===
df_with_time = df_parsed.withColumn("event_time", (col("event_time") / 1000).cast("timestamp"))

# === Aroon Calculation ===
# Giả sử Aroon được tính trên cửa sổ 14 kỳ (ở đây 14 phút)
# Công thức:
# Aroon Up = ((N - periods_since_high) / N) * 100
# Aroon Down = ((N - periods_since_low) / N) * 100

N = 14  # số kỳ tính Aroon

# Dùng window 14 phút, trượt mỗi 1 phút
aroon_df = (
    df_with_time
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "14 minutes", "1 minute"),
        col("symbol")
    )
    .agg(
        _max("high_price").alias("max_high"),
        _min("low_price").alias("min_low"),
        _max("close_time").alias("latest_close_time"),
        _max("open_time").alias("latest_open_time")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "symbol",
        # Aroon Up & Down ở đây là minh hoạ đơn giản hoá
        expr(f"((1 - (latest_close_time - latest_open_time)/(60*1000*{N})) * 100)").alias("aroon_up"),
        expr(f"((1 - (latest_open_time - latest_close_time)/(60*1000*{N})) * 100)").alias("aroon_down")
    )
)

# === Ghi stream vào PostgreSQL ===
create_table()

query = (
    aroon_df.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/opt/workspace/aroon/checkpoints/aroon_indicator")
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
