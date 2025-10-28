from threading import Thread
import time

# Import existing run() functions from workspace
from trade_stream import run as trade_run
from kline_stream import run as kline_run

def main():
    # list of symbols to stream (adjust if needed)
    symbols = ["BTC_USDT", "ETH_USDT", "BNB_USDT"]

    t_trade = Thread(target=trade_run, args=(symbols,), daemon=True)
    t_kline = Thread(target=kline_run, args=(symbols,), daemon=True)

    t_trade.start()
    t_kline.start()

    print("Started trade and kline streams in parallel.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping streams...")

if __name__ == "__main__":
    main()
