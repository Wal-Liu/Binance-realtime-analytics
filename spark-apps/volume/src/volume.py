from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
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


def write_to_postgres(batch_df, batch_id):
    try:
        if (batch_df.count() == 0 ):
            return # only write if data exists
        create_table() # ensure table are created

        conn = psycopg2.connect(
            host = "postgres",
            port = 5432,
            database = "crypto_db",
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

def create_table():
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="crypto_db",
            user="postgres",
            password="your_password"
        )
        cursor = conn.cursor()
        delete_old = """DROP TABLE IF EXISTS crypto_volume;"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS crypto_volume (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            symbol VARCHAR(20),
            total_volume NUMERIC,
            UNIQUE (window_start, window_end, symbol)
        );
        """
        create_index_query = " CREATE INDEX IF NOT EXISTS idx_window_start ON crypto_volume(window_start);"
        # cursor.execute(delete_old)
        cursor.execute(create_table_query)
        cursor.execute(create_index_query)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating table: {e}")

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

# 4. Parse dữ liệu JSON từ Kafka value
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# 5. Chuyển timestamp từ milliseconds sang dạng thời gian Spark
df_with_time = df_parsed.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

# 6. Tính volume theo cửa sổ thời gian và loại crypto
volume_df = (
    df_with_time
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),  # cửa sổ trượt 1 phút, cập nhật mỗi 30s
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

# 7. Ghi kết quả ra console (có thể thay = Kafka, Delta, hoặc ClickHouse)
query = (
    volume_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
