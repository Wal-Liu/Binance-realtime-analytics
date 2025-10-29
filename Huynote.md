/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic binance_kline_streams --from-beginning --max-messages 5

/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic binance_trade_streams --from-beginning --max-messages 5

psql -U postgres -d crypto_db

| Mục tiêu                             | Lệnh                        |
| ------------------------------------ | --------------------------- |
| Liệt kê database                     | `\l`                        |
| Chuyển sang database khác            | `\c <tên_db>`               |
| Liệt kê bảng trong database hiện tại | `\dt`                       |
| Liệt kê bảng, view, sequence, v.v.   | `\d`                        |
| Xem chi tiết cấu trúc bảng           | `\d <tên_bảng>`             |
| Chạy câu lệnh SQL                    | `SELECT * FROM <tên_bảng>;` |
| Thoát khỏi psql                      | `\q`                        |



CREATE TABLE IF NOT EXISTS kline_raw (
    symbol TEXT,
    interval TEXT,
    open_time BIGINT,
    close_time BIGINT,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    number_of_trades INTEGER,
    is_closed BOOLEAN,
    taker_buy_volume DOUBLE PRECISION,
    taker_buy_quote_volume DOUBLE PRECISION,
    event_time BIGINT
);


spark = (
    SparkSession.builder
    .appName("ComputeIndicators")
    .getOrCreate()
)

stream_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
)

parsed_df = stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = (
    parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .start()
)

query.awaitTermination()



docker exec binance-spark-master /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0   /opt/workspace/momentum/src/MFI.py