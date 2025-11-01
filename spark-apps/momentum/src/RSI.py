# RSI.py
# Ứng dụng Spark Streaming tự động tạo bảng Postgres nếu cần.

import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, when, lag, avg, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType,
)
from pyspark.sql.utils import AnalysisException

# --- Cấu hình Kafka & Delta ---
KAFKA_TOPIC = "binance_kline_streams"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
DELTA_HISTORY_PATH = "/opt/workspace/momentum/logs/RSIlogs"
RSI_PERIOD = 14

# --- Cấu hình PostgreSQL ---
# Tách riêng các thành phần để psycopg2 và Spark JDBC cùng sử dụng
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "crypto_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your_password" # <<< THAY ĐỔI
POSTGRES_RSI_TABLE = "rsi_indicators"

# Tạo URL JDBC cho Spark
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
# --------------------------------------------------


def create_table_if_not_exists():
    """
    Sử dụng psycopg2 để kết nối và chạy CREATE TABLE IF NOT EXISTS
    trước khi Spark bắt đầu.
    """
    conn = None
    try:
        print(f"Connecting to Postgres at {POSTGRES_HOST}:{POSTGRES_PORT} to ensure table exists...")
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        # Dùng autocommit để lệnh CREATE TABLE được thực thi ngay lập tức
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Câu lệnh SQL (đã lấy từ các câu trả lời trước)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_RSI_TABLE} (
            symbol VARCHAR(20),
            interval VARCHAR(10),
            close_time BIGINT,
            close_price DECIMAL(16, 8),
            rsi DOUBLE PRECISION,
            PRIMARY KEY (symbol, interval, close_time)
        );
        """
        
        cur.execute(create_table_query)
        print(f"Table '{POSTGRES_RSI_TABLE}' is ready.")
        cur.close()
        
    except psycopg2.OperationalError as e:
        print(f"\n[FATAL ERROR] Could not connect to PostgreSQL: {e}")
        print("Please check if PostgreSQL is running, accessible from this container,")
        print("and if the connection details (host, user, password) are correct.\n")
        sys.exit(1) # Thoát chương trình nếu không kết nối được DB
    except Exception as e:
        print(f"An unexpected error occurred during table creation: {e}")
    finally:
        if conn:
            conn.close()

def get_kline_schema():
    """Trả về schema cho dữ liệu nến Binance."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("close_time", LongType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("is_closed", BooleanType(), True),
    ])

def process_rsi_batch(batch_df, batch_id):
    """Xử lý mỗi batch để tính RSI và lưu vào Postgres."""
    print(f"--- RSI Batch ID: {batch_id} ---")
    
    if batch_df.isEmpty():
        print("RSI Batch is empty.")
        return

    batch_df.persist()

    try:
        df_history = spark.read.format("delta").load(DELTA_HISTORY_PATH)
        df_history = df_history.select(get_kline_schema().names)
        df_combined = df_history.unionByName(batch_df)
    except AnalysisException:
        print("RSI Delta history not found. Starting with current batch.")
        df_combined = batch_df

    df_combined = df_combined.dropDuplicates(["symbol", "interval", "close_time"])
    
    # --- Tính toán RSI (Giữ nguyên) ---
    window_spec = Window.partitionBy("symbol", "interval").orderBy("close_time")
    df_change = df_combined.withColumn(
        "change", col("close_price") - lag("close_price", 1).over(window_spec)
    )
    df_gain_loss = df_change.withColumn(
        "gain", when(col("change") > 0, col("change")).otherwise(0)
    ).withColumn(
        "loss", when(col("change") < 0, -col("change")).otherwise(0)
    )
    window_rsi = window_spec.rowsBetween(-(RSI_PERIOD - 1), 0)
    df_avg_gain_loss = df_gain_loss.withColumn(
        "avg_gain", avg("gain").over(window_rsi)
    ).withColumn(
        "avg_loss", avg("loss").over(window_rsi)
    )
    df_final = df_avg_gain_loss.withColumn(
        "rs", col("avg_gain") / col("avg_loss")
    ).withColumn(
        "rsi",
        when(col("avg_loss") == 0, 100.0)
        .otherwise(100.0 - (100.0 / (1.0 + col("rs"))))
    )

    # --- Lọc kết quả cho batch hiện tại ---
    df_results = df_final.join(
        batch_df.select("symbol", "interval", "close_time").distinct(),
        ["symbol", "interval", "close_time"],
        "inner"
    )
    
    # --- CHUẨN BỊ GHI VÀO POSTGRES ---
    df_to_save = df_results.select(
        "symbol", "interval", "close_time", "close_price", "rsi"
    )
    
    print(f"Calculated RSI for Batch {batch_id}. Saving to PostgreSQL...")
    df_to_save.show(truncate=False)

    try:
        (
            df_to_save.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", POSTGRES_RSI_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        print(f"Successfully saved Batch {batch_id} to PostgreSQL table {POSTGRES_RSI_TABLE}.")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")

    # --- Lưu trạng thái cho batch tiếp theo ---
    batch_df.select(get_kline_schema().names).write.format(
        "delta"
    ).mode("append").save(DELTA_HISTORY_PATH)
    
    batch_df.unpersist()


if __name__ == "__main__":
    
    # --- BƯỚC 1: Đảm bảo bảng tồn tại TRƯỚC KHI chạy Spark ---
    create_table_if_not_exists()
    # -----------------------------------------------------

    # --- BƯỚC 2: Khởi tạo Spark Session ---
    spark = (
        SparkSession.builder.appName("RSIStreamingApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Đọc từ Kafka
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    schema = get_kline_schema()
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )
    df_kline = df_parsed.select("data.*").filter(col("is_closed") == True)

    # Chạy stream
    query = (
        df_kline.writeStream.foreachBatch(process_rsi_batch)
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )

    print(f"RSI Streaming query started. Listening to {KAFKA_TOPIC}")
    print(f"State stored in: {DELTA_HISTORY_PATH}")
    query.awaitTermination()