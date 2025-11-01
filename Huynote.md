## Check Kafka
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```
```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic binance_kline_streams --from-beginning --max-messages 5
```
```bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic binance_trade_streams --from-beginning --max-messages 5
```
## Postgres
```bash
psql -U postgres -d crypto_db
```
| Mục tiêu                             | Lệnh                        |
| ------------------------------------ | --------------------------- |
| Liệt kê database                     | `\l`                        |
| Chuyển sang database khác            | `\c <tên_db>`               |
| Liệt kê bảng trong database hiện tại | `\dt`                       |
| Liệt kê bảng, view, sequence, v.v.   | `\d`                        |
| Xem chi tiết cấu trúc bảng           | `\d <tên_bảng>`             |
| Chạy câu lệnh SQL                    | `SELECT * FROM <tên_bảng>;` |
| Thoát khỏi psql                      | `\q`                        |


## Test Spark Kafka

test.py
```py
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
```


## Run Money Flow Index
```bash
docker exec binance-spark-master /opt/spark/bin/spark-submit   --master spark://spark-master:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0   /opt/workspace/momentum/src/MFI.py
```

> Vo Grafana import file spark-apps/trend/dashboard/MFI.json

## Run Donchian Channel
```bash
docker exec binance-spark-master /opt/spark/bin/spark-submit   --master spark://spark-master:7077    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0  /opt/workspace/trend/src/DonchianChannel.py
```

> Vo container Postgres run
```sql
ALTER TABLE donchian_channel
ADD COLUMN breakout_signal TEXT;

UPDATE donchian_channel
SET breakout_signal =
  CASE
    WHEN close_price > upper_band THEN 'BUY'
    WHEN close_price < lower_band THEN 'SELL'
    ELSE 'HOLD'
  END
WHERE breakout_signal IS NULL;

ALTER TABLE donchian_channel
ALTER COLUMN breakout_signal SET DEFAULT 'HOLD';

ALTER TABLE donchian_channel
ALTER COLUMN breakout_signal SET NOT NULL;
```

> Vo Grafana import file spark-apps/trend/dashboard/MFI.json
