import pandas as pd
from typing import Iterator, Any

from pyspark.sql import SparkSession, Row
# THÊM 'stddev_pop' (Độ lệch chuẩn) và 'lit' (Giá trị hằng số)
from pyspark.sql.functions import col, from_json, window, avg, sum, stddev_pop, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DecimalType, BooleanType, DoubleType
)

def main():
    spark = SparkSession.builder \
        .appName("BinanceBollingerBands") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Schema cho Kafka JSON (Giống như trước)
    kline_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("close_time", LongType(), True),
        StructField("close_price", DecimalType(20, 10)),
        StructField("is_closed", BooleanType(), True),
        # (Các trường khác)
        StructField("volume", DoubleType(), True),
    ])

    # 2. Đọc stream từ Kafka (Giống như trước)
    kafka_raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "binance-kafka:9092") \
        .option("subscribe", "binance_kline_streams") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Giải mã JSON, Lọc, và Thêm Timestamp (Giống như trước)
    kline_df = kafka_raw_df.select(
        from_json(col("value").cast("string"), kline_schema).alias("kline_data")
    ).select("kline_data.*")

    kline_with_timestamp_df = kline_df \
        .filter(col("is_closed") == True) \
        .withColumn("event_timestamp", (col("close_time") / 1000).cast("timestamp"))

    # 4. === LOGIC AGGREGATION (CẬP NHẬT) ===
    # Tính các thành phần cơ bản (Trung bình VÀ Độ lệch chuẩn)
    agg_df = kline_with_timestamp_df \
        .groupBy(
            window(col("event_timestamp"), "20 minutes", "1 minute"), 
            col("symbol")
        ) \
        .agg(
            # Đổi tên avg_close_price thành 'middle_band' cho rõ nghĩa
            avg("close_price").alias("middle_band"),
            # TÍNH THÊM: Độ lệch chuẩn của giá đóng cửa
            stddev_pop("close_price").alias("std_dev"),
            sum("volume").alias("total_volume")
        )

    # 5. === BƯỚC MỚI: TÍNH TOÁN BOLLINGER BANDS ===
    # Dùng hằng số K = 2
    k = 2
    bollinger_bands_df = agg_df.withColumn(
        "upper_band", col("middle_band") + (lit(k) * col("std_dev"))
    ).withColumn(
        "lower_band", col("middle_band") - (lit(k) * col("std_dev"))
    )

    # 6. Ghi ra Console
    # Chúng ta sẽ ghi 'bollinger_bands_df' thay vì 'agg_df'
    query = bollinger_bands_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()