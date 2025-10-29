# stochastic_oscillator.py
# Ứng dụng Spark Streaming tính toán Stochastic Oscillator và LƯU VÀO POSTGRESQL.

import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, when, lag, avg, min, max, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType,
)
from pyspark.sql.utils import AnalysisException

# --- Cấu hình Kafka & Delta ---
KAFKA_TOPIC = "binance_kline_streams"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
# !!! THAY ĐỔI QUAN TRỌNG: Đường dẫn Delta phải riêng biệt với RSI !!!
DELTA_HISTORY_PATH = "/opt/workspace/momentum/logs/Stochasticlogs"
STOCHASTIC_PERIOD = 14
STOCHASTIC_D_PERIOD = 3

# --- Cấu hình PostgreSQL ---
# (Sao chép từ file RSI.py của bạn)
POSTGRES_URL = "jdbc:postgresql://postgres:5432/crypto_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "your_password" # <<< THAY ĐỔI
POSTGRES_STOCHASTIC_TABLE = "stochastic_indicators" # Tên bảng bạn đã tạo ở Bước 1
# --------------------------------------------------

def get_kline_schema():
    """Trả về schema cho dữ liệu nến Binance."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("close_time", LongType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("is_closed", BooleanType(), True),
    ])

def process_stochastic_batch(batch_df, batch_id):
    """Xử lý mỗi batch để tính Stochastic và lưu vào Postgres."""
    print(f"--- Stochastic Batch ID: {batch_id} ---")
    
    if batch_df.isEmpty():
        print("Stochastic Batch is empty.")
        return

    batch_df.persist()

    try:
        df_history = spark.read.format("delta").load(DELTA_HISTORY_PATH)
        df_history = df_history.select(get_kline_schema().names)
        df_combined = df_history.unionByName(batch_df)
    except AnalysisException:
        print("Stochastic Delta history not found. Starting with current batch.")
        df_combined = batch_df

    df_combined = df_combined.dropDuplicates(["symbol", "interval", "close_time"])
    
    # --- Tính toán Stochastic (Giữ nguyên) ---
    window_spec = Window.partitionBy("symbol", "interval").orderBy("close_time")

    window_stoch = window_spec.rowsBetween(-(STOCHASTIC_PERIOD - 1), 0)
    df_stoch_hl = df_combined.withColumn(
        "low_14", min("low_price").over(window_stoch)
    ).withColumn(
        "high_14", max("high_price").over(window_stoch)
    )
    df_percent_k = df_stoch_hl.withColumn(
        "percent_k",
        when(
            (col("high_14") - col("low_14")) == 0, 50.0
        ).otherwise(
            100.0 * (
                (col("close_price") - col("low_14")) /
                (col("high_14") - col("low_14"))
            )
        )
    )
    window_stoch_d = window_spec.rowsBetween(-(STOCHASTIC_D_PERIOD - 1), 0)
    df_final = df_percent_k.withColumn(
        "percent_d", avg("percent_k").over(window_stoch_d)
    )

    # --- Lọc kết quả cho batch hiện tại ---
    df_results = df_final.join(
        batch_df.select("symbol", "interval", "close_time").distinct(),
        ["symbol", "interval", "close_time"],
        "inner"
    )
    
    # --- CHUẨN BỊ GHI VÀO POSTGRES ---
    df_to_save = df_results.select(
        "symbol", "interval", "close_time", "close_price", "percent_k", "percent_d"
    )
    
    print(f"Calculated Stochastic for Batch {batch_id}. Saving to PostgreSQL...")
    df_to_save.show(truncate=False)

    try:
        (
            df_to_save.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", POSTGRES_STOCHASTIC_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver") # Driver cho Postgres
            .mode("append") # Thêm dữ liệu mới
            .save()
        )
        print(f"Successfully saved Batch {batch_id} to PostgreSQL table {POSTGRES_STOCHASTIC_TABLE}.")
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")


    # --- Lưu trạng thái cho batch tiếp theo ---
    batch_df.select(get_kline_schema().names).write.format(
        "delta"
    ).mode("append").save(DELTA_HISTORY_PATH)
    
    batch_df.unpersist()

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("StochasticStreamingApp")
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

    # Chỉ parse các trường cần thiết cho Stochastic
    schema = get_kline_schema()
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )

    # Chỉ xử lý nến đã đóng
    df_kline = df_parsed.select("data.*").filter(col("is_closed") == True)

    # Chạy stream
    query = (
        df_kline.writeStream.foreachBatch(process_stochastic_batch)
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .start()
    )

    print(f"Stochastic Streaming query started. Listening to {KAFKA_TOPIC}")
    print(f"State stored in: {DELTA_HISTORY_PATH}")
    query.awaitTermination()