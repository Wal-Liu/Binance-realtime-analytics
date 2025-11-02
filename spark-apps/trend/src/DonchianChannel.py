import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType, TimestampType
)
from pyspark.sql.window import Window


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_SOURCE_TOPIC = "binance_kline_streams"

SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "Donchian_Channel_Streaming"

DONCHIAN_PERIOD = 20

PG_URL = "jdbc:postgresql://postgres:5432/crypto_db"
PG_TABLE = "donchian_channel"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "your_password",
    "driver": "org.postgresql.Driver",
}

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

    # Window Donchian
    donchian_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-DONCHIAN_PERIOD + 1, 0)
    prev_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-DONCHIAN_PERIOD, -1)

    donchian_df = (
        df
        .withColumn("upper_band", F.max("high_price").over(donchian_window))
        .withColumn("lower_band", F.min("low_price").over(donchian_window))
        .withColumn("middle_band", (F.col("upper_band") + F.col("lower_band")) / 2)
        .withColumn("upper_prev", F.max("high_price").over(prev_window))
        .withColumn("lower_prev", F.min("low_price").over(prev_window))
        .withColumn(
            "breakout_signal",
            F.when(F.col("close_price") > F.col("upper_prev"), F.lit("BUY"))
             .when(F.col("close_price") < F.col("lower_prev"), F.lit("SELL"))
             .otherwise(F.lit("HOLD"))
        )
        .filter(F.col("upper_band").isNotNull() & F.col("lower_band").isNotNull())
        .select("symbol", "timestamp", "close_price", "upper_band", "lower_band", "middle_band", "breakout_signal")
    )

    (
        donchian_df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", PG_TABLE)
        .options(**PG_PROPERTIES)
        .mode("append")
        .save()
    )

    print(f"Epoch {epoch_id}: Saved {donchian_df.count()} rows to PostgreSQL table `{PG_TABLE}`")

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

    parsed_df = parsed_df.withColumn("timestamp", (F.col("close_time") / 1000).cast("timestamp"))
    parsed_df = parsed_df.withWatermark("timestamp", "2 minutes")

    query = (
        parsed_df.writeStream
        .foreachBatch(process_donchian_batch)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/spark/donchian_checkpoint")
        .start()
    )

    print(f"Streaming Donchian Channel (N={DONCHIAN_PERIOD}) from `{KAFKA_SOURCE_TOPIC}` → PostgreSQL `{PG_TABLE}` ...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
