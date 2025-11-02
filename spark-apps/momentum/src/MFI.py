import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "binance_kline_streams"
SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "MFI_Streaming_App"

PG_URL = "jdbc:postgresql://postgres:5432/crypto_db"
PG_TABLE = "mfi_14d"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "your_password",
    "driver": "org.postgresql.Driver",
}

MFI_PERIOD = 14

kline_schema = StructType([
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
])

def process_mfi_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        print(f"Epoch {epoch_id}: không có dữ liệu")
        return

    print(f"Epoch {epoch_id}: xử lý MFI")

    batch_df = (
        batch_df
        .withColumn("timestamp", (F.col("close_time") / 1000).cast(TimestampType()))
        .filter(F.col("is_closed") == True)
        .withColumn("typical_price",
            (F.col("high_price") + F.col("low_price") + F.col("close_price")) / 3
        )
        .select("symbol", "timestamp", "typical_price", "volume")
    )

    lag_window = Window.partitionBy("symbol").orderBy("timestamp")
    mf_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-MFI_PERIOD + 1, 0)

    mfi_df = (
        batch_df
        .withColumn("prev_tp", F.lag("typical_price", 1).over(lag_window))
        .withColumn("raw_mf", F.col("typical_price") * F.col("volume"))
        .withColumn("pos_mf", F.when(F.col("typical_price") > F.col("prev_tp"), F.col("raw_mf")).otherwise(0.0))
        .withColumn("neg_mf", F.when(F.col("typical_price") < F.col("prev_tp"), F.col("raw_mf")).otherwise(0.0))
        .withColumn("sum_pos_mf", F.sum("pos_mf").over(mf_window))
        .withColumn("sum_neg_mf", F.sum("neg_mf").over(mf_window))
        .withColumn("mfr", F.when(F.col("sum_neg_mf") != 0, F.col("sum_pos_mf") / F.col("sum_neg_mf")))
        .withColumn("mfi", 100 - (100 / (1 + F.col("mfr"))))
        .select("symbol", "timestamp", F.round("mfi", 2).alias("mfi_value"))
        .filter(F.col("mfi_value").isNotNull())
    )

    count = mfi_df.count()
    print(f"Epoch {epoch_id}: {count} hàng MFI")

    if count > 0:
        mfi_df.write.jdbc(url=PG_URL, table=PG_TABLE, mode="append", properties=PG_PROPERTIES)

def main():
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session sẵn sàng.")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), kline_schema).alias("data")
    ).select("data.*")

    query = (
        parsed_df.writeStream
        .foreachBatch(process_mfi_batch)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/spark/mfi_checkpoint")
        .start()
    )

    print(f"Đang tính MFI realtime từ Kafka topic `{KAFKA_TOPIC}` ...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
