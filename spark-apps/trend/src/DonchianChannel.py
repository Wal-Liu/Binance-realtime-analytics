import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType, TimestampType
)
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_SOURCE_TOPIC = "binance_kline_streams"
KAFKA_OUTPUT_TOPIC = "donchian_signals"

SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "Donchian_Channel_Streaming"

DONCHIAN_PERIOD = 20

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

def process_donchian_batch(batch_df, epoch_id):
    if batch_df.isEmpty():
        print(f"Epoch {epoch_id}: No data")
        return

    print(f"Epoch {epoch_id}: Processing Donchian Channel...")

    df = (
        batch_df
        .withColumn("timestamp", (F.col("close_time") / 1000).cast("timestamp"))
        .filter(F.col("is_closed") == True)
        .select("symbol", "timestamp", "high_price", "low_price", "close_price")
    )

    # Window cho 20 phiên gần nhất
    donchian_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-DONCHIAN_PERIOD + 1, 0)
    prev_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-DONCHIAN_PERIOD, -1)

    donchian_df = (
        df
        .withColumn("upper", F.max("high_price").over(donchian_window))
        .withColumn("lower", F.min("low_price").over(donchian_window))
        .withColumn("middle", (F.col("upper") + F.col("lower")) / 2)
        .withColumn("upper_prev", F.max("high_price").over(prev_window))
        .withColumn("lower_prev", F.min("low_price").over(prev_window))
        .withColumn(
            "signal",
            F.when(F.col("close_price") > F.col("upper_prev"), F.lit("breakout_up"))
             .when(F.col("close_price") < F.col("lower_prev"), F.lit("breakout_down"))
             .otherwise(F.lit("none"))
        )
        .filter(F.col("upper").isNotNull() & F.col("lower").isNotNull())
        .select("symbol", "timestamp", "close_price", "upper", "lower", "middle", "signal")
    )

    # Chỉ gửi tín hiệu breakout
    signals_df = donchian_df.filter(F.col("signal") != "none")

    kafka_out_df = signals_df.select(
        F.to_json(F.struct(
            F.col("symbol"),
            F.col("timestamp"),
            F.col("close_price"),
            F.col("upper"),
            F.col("lower"),
            F.col("middle"),
            F.col("signal")
        )).alias("value")
    )

    kafka_out_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_OUTPUT_TOPIC) \
        .save()

    print(f"Epoch {epoch_id}: Sent {signals_df.count()} signals to topic `{KAFKA_OUTPUT_TOPIC}`")

def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session ready.")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), kline_schema).alias("data")
    ).select("data.*")

    # timestamp watermark (2 phút)
    parsed_df = parsed_df.withColumn("timestamp", (F.col("close_time") / 1000).cast("timestamp"))
    parsed_df = parsed_df.withWatermark("timestamp", "2 minutes")

    query = (
        parsed_df.writeStream
        .foreachBatch(process_donchian_batch)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/spark/donchian_checkpoint")
        .start()
    )

    print(f"Streaming Donchian Channel (N={DONCHIAN_PERIOD}) từ `{KAFKA_SOURCE_TOPIC}` → `{KAFKA_OUTPUT_TOPIC}` ...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
