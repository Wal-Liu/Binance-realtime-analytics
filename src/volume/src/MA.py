from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, avg, when, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import json
from pathlib import Path
import psycopg2


KAFKA_FILE = Path("/opt/workspace/volume/configs/kafka.json")
with open(KAFKA_FILE, "r", encoding="utf-8") as f:
    KAFKA = json.load(f)

POSTGRE_FILE = Path("/opt/workspace/volume/configs/postgre.json")
with open(POSTGRE_FILE, "r", encoding="utf-8") as f:
    POSTGRE = json.load(f)

DEFAULT_THRESHOLD_MULTIPLIER = 1.5  # ngưỡng cảnh báo = MA_5min * hệ số

# -- Bảng volume và alert trong DB

def create_tables():
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="crypto_db",
            user="postgres",
            password="your_password"
        )
        cursor = conn.cursor()
        # Bảng volume
        create_volume_table = """
        CREATE TABLE IF NOT EXISTS crypto_volume (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            symbol VARCHAR(20),
            total_volume NUMERIC,
            UNIQUE (window_start, window_end, symbol)
        );
        """
        cursor.execute(create_volume_table)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_window_start ON crypto_volume(window_start);")

        # Bảng alert
        create_alert_table = """
        CREATE TABLE IF NOT EXISTS crypto_alert (
            id SERIAL PRIMARY KEY,
            alert_time TIMESTAMP NOT NULL,
            symbol VARCHAR(20),
            alert_type VARCHAR(50),
            alert_value NUMERIC
        );
        """
        cursor.execute(create_alert_table)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_alert_time ON crypto_alert(alert_time);")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating tables: {e}")

# Hàm ghi volume vào DB như cũ
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    create_tables()
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="crypto_db",
            user="postgres",
            password="your_password"
        )
        cursor = conn.cursor()
        for row in batch_df.collect():
            cursor.execute(
                """
                INSERT INTO crypto_volume (window_start, window_end, symbol, total_volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (window_start, window_end, symbol)
                DO UPDATE SET total_volume = EXCLUDED.total_volume;
                """,
                (row.window_start, row.window_end, row.symbol, row.total_volume)
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

# Hàm ghi alert vào DB
def write_alert_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    create_tables()
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="crypto_db",
            user="postgres",
            password="your_password"
        )
        cursor = conn.cursor()
        for row in batch_df.collect():
            cursor.execute(
                """
                INSERT INTO crypto_alert (alert_time, symbol, alert_type, alert_value)
                VALUES (%s, %s, %s, %s);
                """,
                (row.window_start, row.symbol, 'Volume Spike', row.total_volume)
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error writing alert to PostgreSQL: {e}")

# 1. Khởi tạo SparkSession
spark = (
    SparkSession.builder
    .appName("CryptoVolumeStream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Đọc stream từ Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA["bootstrap.servers"])
    .option("subscribe", KAFKA["topic"])
    .option("startingOffsets", "earliest")
    .load()
)

# 3. Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("timestamp", LongType())
])

# 4. Parse JSON
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# 5. Chuyển timestamp
df_with_time = df_parsed.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

# 6. Tính volume theo window trượt 1 phút, trượt 30s
volume_df = (
    df_with_time
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("symbol")
    )
    .agg(_sum("quantity").alias("total_volume"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "symbol",
        "total_volume"
    )
)

# 7. Tính MA_5min sử dụng Window Function
window_spec = Window.partitionBy("symbol").orderBy(col("window_start").cast("long")).rangeBetween(-5 * 60, 0)
from pyspark.sql.functions import avg
volume_with_ma = volume_df.withColumn("ma_5min", avg("total_volume").over(window_spec))

# 8. Tạo điều kiện cảnh báo: volume vượt ngưỡng

alert_df = volume_with_ma.withColumn(
    "alert_flag",
    when(col("total_volume") > col("ma_5min") * DEFAULT_THRESHOLD_MULTIPLIER, lit(True)).otherwise(lit(False))
).filter(col("alert_flag") == True).select("window_start", "symbol", "total_volume")

# 9. Start hai streaming query đồng thời
# Ghi volume + MA vào bảng volume
query_volume = (
    volume_with_ma.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

# Ghi cảnh báo khi có alert_flag
query_alert = (
    alert_df.writeStream
    .foreachBatch(write_alert_to_postgres)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

query_volume.awaitTermination()
query_alert.awaitTermination()
