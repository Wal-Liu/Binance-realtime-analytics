import json
# from httpcore import stream
import websocket
from pathlib import Path
import threading
import time
from kafka import KafkaProducer

import os

current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(current_dir)

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
KAFKA_TOPIC = KAFKA["kline_topic"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def make_url(baseurl: str, symbol: str, kline_stream: str) -> str:
    """
    Tạo WebSocket URL hoàn chỉnh từ symbol và kline_stream.
    Ví dụ:
        make_url(SYMBOLS["BTC_USDT"], STREAMS["kline_1m"])
    """
    return f"{baseurl}{symbol}@{kline_stream}"

def delivery_report(err, msg):
    """Callback khi Kafka gửi thành công hoặc lỗi."""
    if err is not None:
        print(f"[KAFKA ERROR] Failed to deliver message: {err}")
    else:
        print(f"[KAFKA OK] Delivered to {msg.topic()} [{msg.partition()}]")

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Kiểm tra gói tin có đủ dữ liệu kline không
        if "k" not in data or not all(k in data["k"] for k in ("o", "h", "l", "c", "v", "T", "t")):
            return

        record = {
            "symbol": data["s"],                        # Tên cặp giao dịch (ví dụ: BTCUSDT)
            "interval": data["k"]["i"],                 # Chu kỳ nến (1m, 5m, 1h, ...)
            "open_time": data["k"]["t"],                # Thời điểm mở nến (epoch ms)
            "close_time": data["k"]["T"],               # Thời điểm đóng nến (epoch ms)
            "open_price": float(data["k"]["o"]),        # Giá mở cửa
            "high_price": float(data["k"]["h"]),        # Giá cao nhất
            "low_price": float(data["k"]["l"]),         # Giá thấp nhất
            "close_price": float(data["k"]["c"]),       # Giá đóng cửa
            "volume": float(data["k"]["v"]),            # Khối lượng giao dịch (base asset)
            "quote_volume": float(data["k"]["q"]),      # Khối lượng quy đổi theo quote asset (VD: USDT)
            "number_of_trades": data["k"]["n"],         # Số lượng giao dịch trong nến
            "is_closed": data["k"]["x"],                # True nếu nến đã hoàn tất
            "taker_buy_volume": float(data["k"]["V"]),  # Volume mua từ taker
            "taker_buy_quote_volume": float(data["k"]["Q"]),  # Quote volume mua từ taker
            "event_time": data["E"],                    # Thời điểm event (ms)
        }

        # Gửi vào Kafka
        producer.send(
            KAFKA_TOPIC,
            value=record,                           # dict -> json -> bytes (nhờ serializer)
            key=data["s"].encode("utf-8")           # key phải là bytes
        )

        print(f"[Kline_Kafka] Sent {record['symbol']} price={record['close_price']}")
    
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


def run(symbols_to_stream):
    threads = []

    for symbol in symbols_to_stream:
        thread = threading.Thread(target=singal_stream, args=(BINANCE_WS_BASE, SYMBOLS[symbol], STREAMS["kline_1m"]))
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

if __name__ == "__main__":
    symbols_to_stream = ["BTC_USDT", "ETH_USDT", "BNB_USDT"]
    run(symbols_to_stream)