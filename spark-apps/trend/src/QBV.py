import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Row
from typing import Iterator # Thêm Iterator cho foreachPartition

# SỬ DỤNG SCHEMA TỪ TỆP UTILS
from utils import kline_schema 

import psycopg2 # Cần 'pip install psycopg2-binary' trên driver
import psycopg2.extras # Thêm import cho batch upsert

# ---- Biến cấu hình ----
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "binance_kline_streams"
SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "QBV_Streaming_App"

PG_HOST = "postgres"
PG_PORT = "5432"
PG_DB = "crypto_db"
PG_USER = "postgres"
PG_PASSWORD = "your_password" # Thay bằng mật khẩu postgres của bạn
PG_TABLE = "qvb_1m"


def create_qvb_table_if_not_exists():
    """
    Kết nối tới Postgres và chạy lệnh CREATE TABLE IF NOT EXISTS cho bảng qvb_1m.
    Khóa chính (start_time, symbol) là bắt buộc cho hoạt động UPSERT.
    """
    # Chuỗi kết nối cho psycopg2
    conn_str = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    
    # Lệnh SQL (kiểu dữ liệu TIMESTAMPZ được khuyên dùng cho thời gian)
    sql_create_table = f"""
        CREATE TABLE IF NOT EXISTS {PG_TABLE} (
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            total_quote_volume NUMERIC(30, 10),
            PRIMARY KEY (start_time, symbol)
        );
        
        COMMENT ON TABLE {PG_TABLE} IS 'Tổng hợp Quote Volume (QBV) mỗi phút từ Spark Streaming.';
        COMMENT ON COLUMN {PG_TABLE}.start_time IS 'Thời gian bắt đầu của cửa sổ 1 phút.';
        COMMENT ON COLUMN {PG_TABLE}.symbol IS 'Ký hiệu cặp giao dịch.';
        COMMENT ON COLUMN {PG_TABLE}.total_quote_volume IS 'Tổng khối lượng quote (ví dụ: USDT) trong 1 phút.';
    """
    
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        cur.execute(sql_create_table)
        conn.commit()
        print(f"Đã xác minh/tạo bảng '{PG_TABLE}' thành công.")
    except Exception as e:
        print(f"Lỗi khi tạo bảng '{PG_TABLE}': {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def write_to_postgres(df: DataFrame, epoch_id: int):
    """
    Hàm này được gọi cho mỗi micro-batch.
    Thực hiện:
    1. Hiển thị thông tin (trên Driver) sử dụng take(1) để tránh lỗi hiệu suất.
    2. Đẩy công việc UPSERT hàng loạt đến Executor bằng foreachPartition.
    """
    
    # Lọc ra các hàng rỗng (Hành động Spark)
    df_filtered = df.filter(F.col("total_quote_volume").isNotNull())
    
    # Chỉ hiển thị nếu DataFrame không rỗng (dùng take(1) an toàn hơn count/collect)
    head_rows = df_filtered.take(1) 
    
    if len(head_rows) == 0:
        print(f"--- [Epoch ID: {epoch_id}] Không có dữ liệu mới để ghi. ---")
        return

    # Hiển thị nhanh trên driver 
    print(f"--- [Epoch ID: {epoch_id}] Chuẩn bị ghi dữ liệu. Xem log Executor để thấy UPSERT thành công. ---")
    df_filtered.show(5, truncate=False)
    
    
    def process_partition(iterator: Iterator[Row]):
        # Khởi tạo kết nối và thực hiện UPSERT trên Executor
        import psycopg2
        import psycopg2.extras
        
        conn = None
        cur = None
        data_tuples = []

        try:
            # 1. Biến đổi dữ liệu từ Spark Row thành Tuples
            for row in iterator:
                 data_tuples.append((
                    row.start_time,
                    row.end_time,
                    row.symbol,
                    row.total_quote_volume
                ))

            # Nếu không có dữ liệu trong phân vùng này, bỏ qua
            if not data_tuples:
                return

            # 2. Thông tin kết nối và SQL
            conn_str = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
            sql_upsert = f"""
                INSERT INTO {PG_TABLE} (
                    start_time, end_time, symbol, total_quote_volume
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (start_time, symbol) 
                DO UPDATE SET
                    end_time = EXCLUDED.end_time,
                    total_quote_volume = EXCLUDED.total_quote_volume;
            """
            
            # 3. Thực thi batch upsert
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()
            
            # Sử dụng execute_batch để tăng hiệu suất
            psycopg2.extras.execute_batch(cur, sql_upsert, data_tuples)
            
            conn.commit()

            # Ghi log số lượng hàng đã được upsert thành công (trên Executor)
            print(f"--- [Epoch ID: {epoch_id}] Đã **UPSERT THÀNH CÔNG** {len(data_tuples)} hàng từ 1 partition.")

        except Exception as e:
            # Lỗi này xảy ra trên worker.
            print(f"!!! Lỗi khi upsert vào PostgreSQL (partition, epoch {epoch_id}): {e}")
            if conn:
                conn.rollback() # Hoàn tác nếu có lỗi
        finally:
            # Đóng kết nối và cursor
            if cur:
                cur.close()
            if conn:
                conn.close()

    # Thực thi hàm 'process_partition' trên mỗi phân vùng
    df_filtered.foreachPartition(process_partition)


def main():
    print("Khởi tạo Spark Session cho QBV...")
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.6.0", # Đảm bảo phiên bản này tồn tại
    ]

    spark = (
        SparkSession.builder.appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session đã sẵn sàng.")

    # === BƯỚC 1: ĐẢM BẢO BẢNG TỒN TẠI TRƯỚC KHI BẮT ĐẦU STREAM ===
    print(f"Đang kiểm tra/tạo bảng PostgreSQL '{PG_TABLE}'...")
    create_qvb_table_if_not_exists()
    print("Kiểm tra bảng hoàn tất.")
    
    # 2. Đọc dữ liệu từ Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 3. Parse JSON và chuyển đổi kiểu dữ liệu
    parsed_df = (
        kafka_df.select(F.col("value").cast("string").alias("json_value"))
        .select(
            F.from_json(F.col("json_value"), kline_schema).alias("data")
        )
        .select("data.*")
    )

    # 4. Xử lý dữ liệu
    processed_df = (
        parsed_df
        # Chỉ xử lý các nến đã đóng
        .filter(F.col("is_closed") == True)
        # Chuyển đổi close_time (ms) sang kiểu Timestamp
        .withColumn(
            "timestamp", (F.col("close_time") / 1000).cast("timestamp")
        )
        # Thêm watermark để xử lý dữ liệu trễ (1 phút)
        .withWatermark("timestamp", "1 minute")
    )

    # 5. Tính toán QBV (Quote Volume)
    # Tổng hợp quote_volume theo cửa sổ 1 phút (Tumbling Window)
    qvb_df = (
        processed_df.groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), "1 minute", "1 minute"),
        )
        # Sử dụng 'quote_volume' cho QBV
        .agg(F.sum("quote_volume").alias("total_quote_volume")) 
        .select(
            F.col("window.start").alias("start_time"),
            F.col("window.end").alias("end_time"),
            F.col("symbol"),
            F.col("total_quote_volume"),
        )
    )

    # 6. Ghi dữ liệu ra Postgres
    # Sử dụng foreachBatch và logic UPSERT
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
