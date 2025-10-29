# Binance-realtime-analytics

# Run QBV
```
docker exec binance-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  /opt/workspace/trend/src/QBV.py
```

# Run BB
```
docker exec binance-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  /opt/workspace/trend/src/BB.py
```

## Create table in Postgres
- Access Postgres Container
```
docker exec -it binance-postgres psql -U postgres -d crypto_db
```

- Create table for QBV and BB
```
-- Bảng cho QBV (Quote Volume)
CREATE TABLE IF NOT EXISTS qvb_1m (
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    symbol VARCHAR(20),
    total_quote_volume DOUBLE PRECISION
);

-- Bảng cho Bollinger Bands
CREATE TABLE IF NOT EXISTS bollinger_bands_1m (
    timestamp TIMESTAMP,
    symbol VARCHAR(20),
    close_price DOUBLE PRECISION,
    sma DOUBLE PRECISION,
    stddev DOUBLE PRECISION,
    upper_band DOUBLE PRECISION,
    lower_band DOUBLE PRECISION
);

-- (Tùy chọn) Tạo index để tăng tốc độ truy vấn cho Grafana
CREATE INDEX IF NOT EXISTS idx_qvb_time ON qvb_1m (symbol, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_bb_time ON bollinger_bands_1m (symbol, timestamp DESC);
```
