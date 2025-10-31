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
