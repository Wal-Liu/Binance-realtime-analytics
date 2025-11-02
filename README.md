# Real-time Crypto Market Data Processing System

## Teamate
| Student ID | Name              | GitHub       | Task                                                  |
| ---------- | ----------------- | ------------ | ----------------------------------------------------- |
| 22133026   | Nguyễn Quốc Huy   | @huy-dataguy | Money Flow Index (MFI) & Donchian Channels (DC)       |
| 22133029   | Nguyễn Nam Hy     | @ngnamhy     | Aroon Indicator & Average Directional Index (ADX)     |
| 22133045   | Nguyễn Minh Quang | @DOCUTEE     | On-Balance Volume (OBV) & Bollinger Bands             |
| 22133056   | Nguyễn Quốc Thịnh | @Jayus52Hz   | Relative Strength Index (RSI) & Stochastic Oscillator |
| 22133064   | Lưu Vĩnh Tường    | @Wal-Liu     | Volume & Moving Average (MA)                          | 
## Introduction
Our team builds a `real-time data pipeline` system to automatically collect, calculate financial technical indicators, and instantly visualize market analysis results in the cryptocurrency market. This system helps users monitor and evaluate the market for faster and more accurate decision-making.

## Data Source
The primary data source is the `Binance WebSocket Streams`, which provide candlestick data for specific trading pairs every second (UTC+0). The data includes open time, open price, high price, low price, close price, trading volume, close time, quote asset volume, number of trades, and active buyer volume.

Specifically, we use two main data types:
- `trade` (actual trades)
- `kline_1m` (candlestick data updated every minute)

Tracking data for three coins: `BTC`, `BNB`, `ETH`.

## Technical Indicators Calculated
- Trend
- Momentum
- Volume
- Volatility

## System Architecture
- **Ingestion:** Python connects to Binance [translate:WebSocket] to fetch data and push into [translate:Apache Kafka].
- **Stream Processing:** [translate:Apache Spark cluster] computes indicators, monitors, and detects anomalies in data streams.
- **Storage & Visualization:** Data stored in [translate:PostgreSQL]; analysis results visualized using [translate:Grafana].

## Demo

### Volume and MA 
https://github.com/user-attachments/assets/5afb524f-54a6-4708-a5b8-4ec03f9339b5

## Technologies Used
- Python, Apache Kafka, Apache Spark
- PostgreSQL, Grafana
- Binance WebSocket API

## Usage Instructions

### QBV
```
docker exec binance-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  /opt/workspace/trend/src/QBV.py
```

### BB
```
docker exec binance-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  /opt/workspace/trend/src/BB.py
```

#### Create table in Postgres
- Access Postgres Container
```
docker exec -it binance-postgres psql -U postgres -d crypto_db
```

### Volume and MA
```
docker exec binance-spark-master ./volume/src/submit.sh
```