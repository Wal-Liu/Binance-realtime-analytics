from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import json
from pathlib import Path


KAFKA_FILE = Path("/opt/workspace/volume/configs/kafka.json")
with open(KAFKA_FILE, "r", encoding="utf-8") as f:
    KAFKA = json.load(f)

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
    .option("startingOffsets", "latest")
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
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 50)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
