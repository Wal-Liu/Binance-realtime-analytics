import json
import websocket
from pathlib import Path
import threading
import time
from kafka import KafkaProducer

BINANCE_FILE = Path("./configs/binance.json")
KAFKA_FILE = Path("./configs/kafka.json")

with open(BINANCE_FILE, "r", encoding="utf-8") as f:
        BINANCE = json.load(f)

BINANCE_WS_BASE = BINANCE["websocket_base"]
STREAMS = BINANCE["streams"]
SYMBOLS = BINANCE["symbols"]

with open(KAFKA_FILE, "r", encoding="utf-8") as f:
    KAFKA = json.load(f)

KAFKA_BOOTSTRAP = KAFKA["bootstrap.servers"]
KAFKA_TOPIC = KAFKA["topic"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def make_url(baseurl: str, symbol: str, stream: str) -> str:
    """
    Tạo WebSocket URL hoàn chỉnh từ symbol và stream.
    Ví dụ:
        make_url(SYMBOLS["BTC_USDT"], STREAMS["trade"])
    """
    return f"{baseurl}{symbol}@{stream}"

def delivery_report(err, msg):
    """Callback khi Kafka gửi thành công hoặc lỗi."""
    if err is not None:
        print(f"[KAFKA ERROR] Failed to deliver message: {err}")
    else:
        print(f"[KAFKA OK] Delivered to {msg.topic()} [{msg.partition()}]")

def on_message(ws, message):
    try:
        data = json.loads(message)

        if not all(k in data for k in ("s", "p", "q", "T")):
            return

        record = {
            "symbol": data["s"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "timestamp": data["T"],
        }

        # Gửi vào Kafka
        producer.send(
            KAFKA_TOPIC,
            value=record,                           # dict -> json -> bytes (nhờ serializer)
            key=data["s"].encode("utf-8")           # key phải là bytes
        )

        print(f"[KAFKA] Sent {record['symbol']} price={record['price']}")
    
    except Exception as e:
        print(f"Raw message: {message}")
        print(f"Error processing message: {type(e).__name__}: {e}")



def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Closed connection")

def on_open(ws):
    print("Connected to Binance Trade Stream")

def singal_stream(baseurl: str, symbol: str, stream: str):
    url = make_url(baseurl, symbol, stream)
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    while True:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"[{symbol}] Connection error: {e}, retrying in 5s...")
            time.sleep(5)  # Wait before reconnecting


if __name__ == "__main__":
    

    symbols_to_stream = ["BTC_USDT", "ETH_USDT", "BNB_USDT"]

    threads = []

    for symbol in symbols_to_stream:
        thread = threading.Thread(target=singal_stream, args=(BINANCE_WS_BASE, SYMBOLS[symbol], STREAMS["trade"]))
        thread.start()
        threads.append(thread)
        print(f"[THREAD] Started stream for {symbol}")
        time.sleep(0.5)  # tránh spam kết nối cùng lúc
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping all threads...")
    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")
