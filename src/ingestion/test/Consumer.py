# consumer_metadata.py
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import os
from datetime import datetime
import json

# ---------- Config ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
KAFKA_TOPIC_META = os.getenv("KAFKA_TOPIC_META", "binance_streams")
SAVE_DIR = os.getenv("METADATA_SAVE_DIR", "./src/ingestion/logs")

os.makedirs(SAVE_DIR, exist_ok=True)
LOG_FILE = os.path.join(SAVE_DIR, f"kafka_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")



# ---------- Kafka Consumer ----------
def consume_metadata():
    print(f"[INFO] Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC_META}")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC_META,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id="metadata_consumer_group",
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
    )

    try:
        print("[INFO] Connected successfully. Waiting for messages...\n")
        for msg in consumer:
            record = msg.value
            key = msg.key or "unknown"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Tạo thư mục riêng cho từng key
            key_dir = os.path.join(SAVE_DIR, key)
            os.makedirs(key_dir, exist_ok=True)

            # Ghi vào file riêng theo ngày
            file_path = os.path.join(key_dir, f"{key}_{datetime.now().strftime('%Y%m%d')}.log")
            log_line = f"[{timestamp}] {json.dumps(record, ensure_ascii=False)}\n"

            with open(file_path, "a", encoding="utf-8") as f:
                f.write(log_line)
                f.flush()

            print(f"[{key}] {record}")
    except KeyboardInterrupt:
        print("\n[STOP] Consumer stopped by user.")
    except Exception as e:
        print(f"[ERROR] {e}")


# ---------- Main ----------
if __name__ == "__main__":
    consume_metadata()
