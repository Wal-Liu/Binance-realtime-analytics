import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)

# ---- Biến cấu hình ----
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "binance_kline_streams"
SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "QBV_Streaming_App"

PG_URL = "jdbc:postgresql://postgres:5432/crypto_db"
PG_TABLE = "qvb_1m"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "your_password",  # Thay bằng mật khẩu postgres của bạn
    "driver": "org.postgresql.Driver",
}

# ---- Schema cho dữ liệu K-line từ Binance ----
kline_schema = StructType(
    [
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
    ]
)


def write_to_postgres(df, epoch_id):
    """
    Hàm ghi một micro-batch DataFrame vào Postgres.
    """
    print(f"--- Đang ghi Epoch ID: {epoch_id} ---")
    # Print schema và một vài dòng dữ liệu để debug
    # df.printSchema()
    df.show(5, truncate=False)
    df.write.jdbc(
        url=PG_URL, table=PG_TABLE, mode="append", properties=PG_PROPERTIES
    )


def main():
    print("Khởi tạo Spark Session cho QBV...")

    # Cần thêm package cho Kafka và PostgreSQL
    # Spark 3.5.0 (phiên bản Spark phổ biến) tương ứng với Kafka 4.0.1
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.6.0",  # JDBC Driver cho Postgres
    ]

    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session đã sẵn sàng.")

    # 1. Đọc dữ liệu từ Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Parse JSON và chuyển đổi kiểu dữ liệu
    parsed_df = (
        kafka_df.select(F.col("value").cast("string").alias("json_value"))
        .select(
            F.from_json(F.col("json_value"), kline_schema).alias("data")
        )
        .select("data.*")
    )

    # 3. Xử lý dữ liệu
    processed_df = (
        parsed_df
        # Chỉ xử lý các nến đã đóng
        .filter(F.col("is_closed") == True)
        # Chuyển đổi close_time (ms) sang kiểu Timestamp
        .withColumn(
            "timestamp", (F.col("close_time") / 1000).cast(TimestampType())
        )
        # Thêm watermark để xử lý dữ liệu trễ (1 phút)
        .withWatermark("timestamp", "1 minute")
    )

    # 4. Tính toán QBV (Quote Volume)
    # Tổng hợp quote_volume theo cửa sổ 1 phút (tương ứng với interval 1m)
    qvb_df = (
        processed_df.groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), "1 minute", "1 minute"),
        )
        .agg(F.sum("quote_volume").alias("total_quote_volume"))
        .select(
            F.col("window.start").alias("start_time"),
            F.col("window.end").alias("end_time"),
            F.col("symbol"),
            F.col("total_quote_volume"),
        )
    )

    # 5. Ghi dữ liệu ra Postgres
    # Sử dụng foreachBatch để ghi vào JDBC sink
    query = (
        qvb_df.writeStream.outputMode("update")  # 'update' cho window aggregation
        .foreachBatch(write_to_postgres)
        .trigger(processingTime="1 minute")  # Chạy mỗi phút
        .start()
    )

    print(f"Đang lắng nghe topic {KAFKA_TOPIC}...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
