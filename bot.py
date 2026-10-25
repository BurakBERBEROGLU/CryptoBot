import csv
import time
from pathlib import Path

import ccxt
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator

# Binance public API
exchange = ccxt.binance({"enableRateLimit": True})

SYMBOL = "BTC/USDT"
TIMEFRAME = "15m"
LIMIT = 100
TRADE_LOG_FILE = Path("paper_trades.csv")

POSITION = None


def init_trade_log():
    if not TRADE_LOG_FILE.exists() or TRADE_LOG_FILE.stat().st_size == 0:
        with TRADE_LOG_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "time",
                "symbol",
                "side",
                "entry_price",
                "exit_price",
                "pnl",
                "tp_price",
                "sl_price",
                "reason"
            ])


def load_market_data():
    ohlcv = exchange.fetch_ohlcv(
        SYMBOL,
        timeframe=TIMEFRAME,
        limit=LIMIT
    )

    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    df["EMA20"] = EMAIndicator(close=df["close"], window=20).ema_indicator()
    df["EMA50"] = EMAIndicator(close=df["close"], window=50).ema_indicator()
    df["RSI"] = RSIIndicator(close=df["close"], window=14).rsi()

    return df


def get_signal(df):
    last = df.iloc[-1]

    if last["EMA20"] > last["EMA50"] and last["RSI"] < 70:
        return "LONG"
    if last["EMA20"] < last["EMA50"] and last["RSI"] > 30:
        return "SHORT"
    return "WAIT"


def open_position(side, entry_price, tp_price, sl_price):
    global POSITION
    POSITION = {
        "side": side,
        "entry_price": float(entry_price),
        "tp_price": float(tp_price),
        "sl_price": float(sl_price),
        "opened_at": time.time(),
    }
    print(f"[{time.strftime('%H:%M:%S')}] {side} açıldı | giriş={entry_price:.2f} | TP={tp_price:.2f} | SL={sl_price:.2f}")


def close_position(exit_price, reason, pnl=0.0):
    global POSITION
    if POSITION is None:
        return

    side = POSITION["side"]
    entry_price = POSITION["entry_price"]

    with TRADE_LOG_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            time.strftime("%Y-%m-%d %H:%M:%S"),
            SYMBOL,
            side,
            entry_price,
            exit_price,
            pnl,
            POSITION["tp_price"],
            POSITION["sl_price"],
            reason,
        ])

    print(f"[{time.strftime('%H:%M:%S')}] {side} kapandı | fiyat={exit_price:.2f} | pnl={pnl:.2f} | neden={reason}")
    POSITION = None


def check_exit(last_price):
    if POSITION is None:
        return

    side = POSITION["side"]
    if side == "LONG":
        if last_price >= POSITION["tp_price"]:
            pnl = (last_price - POSITION["entry_price"]) * 1.0
            close_position(last_price, "TP", pnl)
            return
        if last_price <= POSITION["sl_price"]:
            pnl = (last_price - POSITION["entry_price"]) * 1.0
            close_position(last_price, "SL", pnl)
            return
    else:
        if last_price <= POSITION["tp_price"]:
            pnl = (POSITION["entry_price"] - last_price) * 1.0
            close_position(last_price, "TP", pnl)
            return
        if last_price >= POSITION["sl_price"]:
            pnl = (POSITION["entry_price"] - last_price) * 1.0
            close_position(last_price, "SL", pnl)
            return


def paper_trade_loop(max_cycles=5, sleep_seconds=10):
    init_trade_log()
    last_signal = "WAIT"

    for cycle in range(max_cycles):
        try:
            df = load_market_data()
            last = df.iloc[-1]
            signal = get_signal(df)

            print("\n==============================")
            print(f"Döngü        : {cycle + 1}/{max_cycles}")
            print(f"Tarih        : {last['timestamp']}")
            print(f"BTC fiyatı   : {last['close']:.2f} USDT")
            print(f"EMA20        : {last['EMA20']:.2f}")
            print(f"EMA50        : {last['EMA50']:.2f}")
            print(f"RSI          : {last['RSI']:.2f}")
            print(f"Sinyal       : {signal}")
            print("==============================")

            if POSITION is not None:
                check_exit(last["close"])

            if signal == "WAIT":
                last_signal = signal
                time.sleep(sleep_seconds)
                continue

            if POSITION is None:
                if signal == "LONG":
                    entry = last["close"]
                    tp = entry * 1.02
                    sl = entry * 0.98
                    open_position("LONG", entry, tp, sl)
                elif signal == "SHORT":
                    entry = last["close"]
                    tp = entry * 0.98
                    sl = entry * 1.02
                    open_position("SHORT", entry, tp, sl)
            else:
                if signal != POSITION["side"]:
                    close_position(last["close"], "SIGNAL_FLIP", 0.0)
                    if signal == "LONG":
                        entry = last["close"]
                        tp = entry * 1.02
                        sl = entry * 0.98
                        open_position("LONG", entry, tp, sl)
                    elif signal == "SHORT":
                        entry = last["close"]
                        tp = entry * 0.98
                        sl = entry * 1.02
                        open_position("SHORT", entry, tp, sl)

            last_signal = signal
            time.sleep(sleep_seconds)

        except Exception as exc:
            print(f"Hata: {exc}")
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    paper_trade_loop(max_cycles=5, sleep_seconds=10)
    print("\nPaper trade döngüsü tamamlandı.")