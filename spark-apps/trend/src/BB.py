from typing import Iterator

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.functions import col, from_json, window, avg, sum, stddev_pop, lit, count
from utils import kline_schema, create_table_if_not_exists
import psycopg2 
import psycopg2.extras

def write_to_postgres(df: DataFrame, epoch_id: int):
    print("--- Đang ghi Epoch ID:", epoch_id, "---")
    def process_partition(iterator: Iterator[Row]):
        conn_str = "postgresql://postgres:your_password@binance-postgres:5432/crypto_db"
        
        sql_upsert = """
            INSERT INTO bollinger_bands_1m_agg (
                window_start, window_end, symbol, 
                middle_band, std_dev, upper_band, 
                lower_band, total_volume
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (window_start, symbol) 
            DO UPDATE SET
                window_end = EXCLUDED.window_end,
                middle_band = EXCLUDED.middle_band,
                std_dev = EXCLUDED.std_dev,
                upper_band = EXCLUDED.upper_band,
                lower_band = EXCLUDED.lower_band,
                total_volume = EXCLUDED.total_volume;
        """
        data_tuples = [
            (
                row.window.start,
                row.window.end,
                row.symbol,
                row.middle_band,
                row.std_dev,
                row.upper_band,
                row.lower_band,
                row.total_volume
            ) for row in iterator
        ]
        if not data_tuples:
            return
        
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, sql_upsert, data_tuples)
            conn.commit()
            print(f"--- [Epoch ID: {epoch_id}] Đã upsert thành công {len(data_tuples)} hàng vào DB.")

        except Exception as e:
            print(f"Lỗi khi upsert vào PostgreSQL (partition, epoch {epoch_id}): {e}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    df.foreachPartition(process_partition)


def create_table_if_not_exists():
    conn_str = "postgresql://postgres:your_password@binance-postgres:5432/crypto_db"
    
    # Lệnh SQL để tạo bảng nếu nó chưa tồn tại
    sql_create_table = """
        CREATE TABLE IF NOT EXISTS bollinger_bands_1m_agg (
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            middle_band NUMERIC(20, 10),
            std_dev NUMERIC(20, 10),
            upper_band NUMERIC(20, 10),
            lower_band NUMERIC(20, 10),
            total_volume NUMERIC(30, 10),
            PRIMARY KEY (window_start, symbol)
        );
    """
    
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        cur.execute(sql_create_table)
        conn.commit()
        print("Đã xác minh/tạo bảng 'bollinger_bands_1m_agg' thành công.")
    except Exception as e:
        print(f"Lỗi khi tạo bảng: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def main():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.6.0", # Đảm bảo phiên bản này tồn tại
    ]
    APP_NAME = 'BinanceBollingerBands'
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master("spark://spark-master:7077")
        .config("spark.cores.max", "2")          # Tổng core tối đa toàn job
        .config("spark.executor.cores", "2")     # Mỗi executor dùng 2 core
        .config("spark.executor.instances", "1") # Chỉ tạo 1 executor
        .config("spark.driver.cores", "2")       # Driver cũng chỉ dùng 2 core
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("Đang kiểm tra/tạo bảng PostgreSQL...")
    create_table_if_not_exists()
    print("Kiểm tra bảng hoàn tất.")



    # 2. Đọc stream từ Kafka
    kafka_raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "binance-kafka:9092") \
        .option("subscribe", "binance_kline_streams") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Giải mã JSON, Lọc, và Thêm Timestamp
    kline_df = kafka_raw_df.select(
        from_json(col("value").cast("string"), kline_schema).alias("kline_data")
    ).select("kline_data.*")

    kline_with_timestamp_df = kline_df \
        .filter(col("is_closed") == True) \
        .withColumn("event_timestamp", (col("close_time") / 1000).cast("timestamp"))

    period = 20

    agg_df_unfiltered = kline_with_timestamp_df \
        .groupBy(
            window(col("event_timestamp"), f"{period} minutes", "1 minute"), 
            col("symbol")
        ) \
        .agg(
            avg("close_price").alias("middle_band"),
            stddev_pop("close_price").alias("std_dev"),
            sum("volume").alias("total_volume"),
            count(lit(1)).alias("kline_count") # Đếm số lượng kline
        )
    
    # Áp dụng bộ lọc: Chỉ giữ lại các cửa sổ có đúng 20 kline
    agg_df = agg_df_unfiltered.filter(col("kline_count") == period)

    k = 2
    bollinger_bands_df = agg_df.withColumn(
        "upper_band", col("middle_band") + (lit(k) * col("std_dev"))
    ).withColumn(
        "lower_band", col("middle_band") - (lit(k) * col("std_dev"))
    )

    query = bollinger_bands_df \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_postgres) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

