from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, avg, lit, when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import json
from pathlib import Path
import psycopg2
from collections import defaultdict
from pyspark.sql import Row
import pandas as pd

KAFKA_FILE = Path("/opt/workspace/volume/configs/kafka.json")
with open(KAFKA_FILE, "r", encoding="utf-8") as f:
    KAFKA = json.load(f)

POSTGRE_FILE = Path("/opt/workspace/volume/configs/postgre.json")
with open(POSTGRE_FILE, "r", encoding="utf-8") as f:
    POSTGRE = json.load(f)

THRESHOLD_FILE = Path("/opt/workspace/volume/configs/alert_threshold.json")
with open(THRESHOLD_FILE, "r", encoding="utf-8") as f:
    THRESHOLDS = json.load(f)


latest_ma_threshold = defaultdict(lambda: None)

# -- Tạo các bảng trong DB

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
        # drop_volume_table = "DROP TABLE IF EXISTS crypto_volume;"
        # cursor.execute(drop_volume_table)   
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

        # Bảng MA
        # drop_ma_table = "DROP TABLE IF EXISTS crypto_ma;"
        # cursor.execute(drop_ma_table)
        create_ma_table = """
        CREATE TABLE IF NOT EXISTS crypto_ma (
            id SERIAL PRIMARY KEY,
            time_start TIMESTAMP,
            time_end TIMESTAMP,
            symbol VARCHAR(20),
            ma_type VARCHAR(20),
            ma_value NUMERIC,
            UNIQUE (time_start, time_end, symbol, ma_type)
        );
        """
        cursor.execute(create_ma_table)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ma_time_start ON crypto_ma(time_start);")

        # Bảng alert
        # drop_alert_table = "DROP TABLE IF EXISTS crypto_alert;"
        # cursor.execute(drop_alert_table)
        create_alert_table = """
        CREATE TABLE IF NOT EXISTS crypto_alert (
            id SERIAL PRIMARY KEY,
            alert_time TIMESTAMP NOT NULL,
            symbol VARCHAR(20),
            alert_type VARCHAR(50),
            alert_value NUMERIC,
            old_value NUMERIC
        );
        """
        cursor.execute(create_alert_table)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_alert_time ON crypto_alert(alert_time);")
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating tables: {e}")

# Hàm ghi volume vào DB
def write_volume_to_postgres(batch_df, batch_id):
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
        print(f"Error writing volume to PostgreSQL: {e}")

# Hàm ghi MA vào DB
def write_ma_to_postgres(batch_df, batch_id):
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
                INSERT INTO crypto_ma (time_start, time_end, symbol, ma_type, ma_value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (time_start, time_end, symbol, ma_type)
                DO UPDATE SET ma_value = EXCLUDED.ma_value;
                """,
                (row.time_start, row.time_end, row.symbol, 'MA_5min', row.ma_5min)
            )

            # Kiểm tra alert với ngưỡng hiện tại
            threshold = latest_ma_threshold.get(row.symbol)

            if threshold is None:
                # Cập nhật ngưỡng trong biến toàn cục
                latest_ma_threshold[row.symbol] = row.ma_5min
                print(f"Updated MA threshold for {row.symbol}: {row.ma_5min}")
                continue

            delta = (row.ma_5min - threshold) / threshold

            if abs(delta) >= THRESHOLDS["spike_alert_threshold"]:
                if delta > 0:
                    # Ghi cảnh báo vào bảng crypto_alert
                    cursor.execute("""
                    INSERT INTO crypto_alert(alert_time, symbol, alert_type, alert_value, old_value)
                    VALUES(%s, %s, %s, %s, %s);
                    """, (row.time_start, row.symbol, "MA Up Spike Alert", row.ma_5min, threshold))
                else:
                    # Ghi cảnh báo vào bảng crypto_alert
                    cursor.execute("""
                    INSERT INTO crypto_alert(alert_time, symbol, alert_type, alert_value, old_value)
                    VALUES(%s, %s, %s, %s, %s);
                    """, (row.time_start, row.symbol, "MA Down Spike Alert", row.ma_5min, threshold))
                # Cập nhật ngưỡng trong biến toàn cục
                latest_ma_threshold[row.symbol] = row.ma_5min
                print(f"Updated MA threshold for {row.symbol}: {row.ma_5min}")
                continue
            
            if abs(delta) >= THRESHOLDS["surge_alert_threshold"] :
                cursor.execute("""
                INSERT INTO crypto_alert(alert_time, symbol, alert_type, alert_value, old_value)
                VALUES(%s, %s, %s, %s, %s);
                """, (row.time_start, row.symbol, "MA Surge Alert", row.ma_5min, threshold))
                # Cập nhật ngưỡng trong biến toàn cục
                latest_ma_threshold[row.symbol] = row.ma_5min
                print(f"Updated MA threshold for {row.symbol}: {row.ma_5min}")
                continue

            if abs(delta) >= THRESHOLDS["shift_alert_threshold"] :
                cursor.execute("""
                INSERT INTO crypto_alert(alert_time, symbol, alert_type, alert_value, old_value)
                VALUES(%s, %s, %s, %s, %s);
                """, (row.time_start, row.symbol, "MA Shift Alert", row.ma_5min, threshold))
                # Cập nhật ngưỡng trong biến toàn cục
                latest_ma_threshold[row.symbol] = row.ma_5min
                print(f"Updated MA threshold for {row.symbol}: {row.ma_5min}")
                continue
        print(f"Processed MA batch {batch_id}")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error writing MA to PostgreSQL: {e}")

# 1. Khởi tạo SparkSession
spark = (
    SparkSession.builder
    .appName("CryptoVolumeStream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# 3. Định nghĩa schema cho dữ liệu JSON
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("timestamp", LongType())
])

df_raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA["bootstrap.servers"])
    .option("subscribe", KAFKA["topic"])
    .option("startingOffsets", "latest")
    .load())

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_with_time = df_parsed.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

# ---------------------------
# 1. Volume 1 phút - trượt 30s
# ---------------------------
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

# ---------------------------
# 2. MA_5min (trượt 30s)
# ---------------------------
ma_df = (
    df_with_time
    .groupBy(
        window(col("event_time"), "5 minutes", "30 seconds"),
        col("symbol")
    )
    .agg(avg("price").alias("ma_5min"))
    .select(
        col("window.start").alias("time_start"),
        col("window.end").alias("time_end"),
        "symbol",
        "ma_5min"
    )
)

# 9. Start streaming query đồng thời

# Query 1: Ghi volume vào bảng crypto_volume
query_volume = (
    volume_df.writeStream
    .foreachBatch(write_volume_to_postgres)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

# Query 2: Ghi MA vào bảng crypto_ma
query_ma = (
    ma_df.writeStream
    .foreachBatch(write_ma_to_postgres)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)


# query_volume.awaitTermination()
# query_ma.awaitTermination()
# query_alert.awaitTermination()
spark.streams.awaitAnyTermination()