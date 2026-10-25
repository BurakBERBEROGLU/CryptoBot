import ccxt
import pandas as pd
import time
import os

from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange


# ============================================================
# AYARLAR
# ============================================================

START_BALANCE = 10_000.0

LEVERAGE = 5

TIMEFRAME = "15m"

CANDLE_LIMIT = 150

MIN_VOLUME_USDT = 1_000_000

RISK_PER_TRADE = 0.01

ATR_STOP_MULTIPLIER = 1.5

ATR_TAKE_MULTIPLIER = 3.0

FEE_RATE = 0.001

MAX_POSITIONS = 3

TRADE_FILE = "paper_trades.csv"


# ============================================================
# BINANCE FUTURES
# ============================================================

exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {
        "defaultType": "future"
    }
})


# ============================================================
# PORTFÖY
# ============================================================

balance = START_BALANCE

positions = {}

trade_history = []

period_start_balance = START_BALANCE


# ============================================================
# MARKETLER
# ============================================================

print("\nBinance Futures piyasaları yükleniyor...")

markets = exchange.load_markets()


excluded = {
    "BTC",
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "DAI"
}


symbols = []

for symbol, market in markets.items():

    if not market.get("active"):
        continue

    if market.get("swap") is not True:
        continue

    if market.get("quote") != "USDT":
        continue

    if market.get("base") in excluded:
        continue

    symbols.append(symbol)


print(
    f"Futures altcoin sayısı: {len(symbols)}"
)


# ============================================================
# TICKER
# ============================================================

def get_tickers():

    try:

        return exchange.fetch_tickers(symbols)

    except Exception as error:

        print(
            f"Ticker hatası: {error}"
        )

        return {}


# ============================================================
# ANALİZ
# ============================================================

def analyze_symbol(symbol):

    try:

        ticker = exchange.fetch_ticker(symbol)

        volume_24h = ticker.get("quoteVolume")

        current_price = ticker.get("last")

        change_24h = ticker.get("percentage")


        if not volume_24h:
            return None


        if volume_24h < MIN_VOLUME_USDT:
            return None


        ohlcv = exchange.fetch_ohlcv(
            symbol,
            timeframe=TIMEFRAME,
            limit=CANDLE_LIMIT
        )


        if len(ohlcv) < 60:
            return None


        df = pd.DataFrame(
            ohlcv,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]
        )


        # ====================================================
        # GÖSTERGELER
        # ====================================================

        df["EMA20"] = EMAIndicator(
            df["close"],
            window=20
        ).ema_indicator()


        df["EMA50"] = EMAIndicator(
            df["close"],
            window=50
        ).ema_indicator()


        df["RSI"] = RSIIndicator(
            df["close"],
            window=14
        ).rsi()


        atr_indicator = AverageTrueRange(
            df["high"],
            df["low"],
            df["close"],
            window=14
        )


        df["ATR"] = (
            atr_indicator.average_true_range()
        )


        df["volume_avg"] = (
            df["volume"]
            .rolling(20)
            .mean()
        )


        df["volume_ratio"] = (
            df["volume"]
            / df["volume_avg"]
        )


        # ====================================================
        # SON TAMAMLANMIŞ MUM
        # ====================================================

        last = df.iloc[-2]


        close = float(last["close"])

        ema20 = float(last["EMA20"])

        ema50 = float(last["EMA50"])

        rsi = float(last["RSI"])

        atr = float(last["ATR"])

        volume_ratio = float(
            last["volume_ratio"]
        )


        # ====================================================
        # LONG
        # ====================================================

        long_score = 0


        if ema20 > ema50:
            long_score += 30


        if 45 <= rsi <= 65:
            long_score += 20

        elif 40 <= rsi < 45:
            long_score += 10


        if volume_ratio >= 1.5:
            long_score += 20

        elif volume_ratio >= 1.2:
            long_score += 10


        if change_24h is not None:

            if change_24h >= 5:
                long_score += 20

            elif change_24h >= 2:
                long_score += 10


        if close > ema20:
            long_score += 10


        # ====================================================
        # SHORT
        # ====================================================

        short_score = 0


        if ema20 < ema50:
            short_score += 30


        if 35 <= rsi <= 55:
            short_score += 20

        elif 55 < rsi <= 60:
            short_score += 10


        if volume_ratio >= 1.5:
            short_score += 20

        elif volume_ratio >= 1.2:
            short_score += 10


        if change_24h is not None:

            if change_24h <= -5:
                short_score += 20

            elif change_24h <= -2:
                short_score += 10


        if close < ema20:
            short_score += 10


        # ====================================================
        # SİNYAL
        # ====================================================

        if (
            long_score >= 60
            and long_score > short_score
        ):

            signal = "LONG"

        elif (
            short_score >= 60
            and short_score > long_score
        ):

            signal = "SHORT"

        else:

            signal = "BEKLE"


        return {

            "symbol": symbol,

            "signal": signal,

            "score": max(
                long_score,
                short_score
            ),

            "close": close,

            "atr": atr,

            "candle_time": int(
                last["timestamp"]
            )

        }


    except Exception:

        return None


# ============================================================
# POZİSYON AÇ
# ============================================================

def open_position(data):

    global balance


    symbol = data["symbol"]

    signal = data["signal"]

    price = data["close"]

    atr = data["atr"]

    score = data["score"]


    if symbol in positions:
        return False


    if len(positions) >= MAX_POSITIONS:
        return False


    if atr <= 0:
        return False


    # ========================================================
    # RİSK
    # ========================================================

    risk_amount = (
        balance * RISK_PER_TRADE
    )


    stop_distance = (
        atr * ATR_STOP_MULTIPLIER
    )


    quantity = (
        risk_amount
        / stop_distance
    )


    notional = (
        quantity * price
    )


    max_notional = (
        balance * LEVERAGE * 0.25
    )


    if notional > max_notional:

        notional = max_notional

        quantity = (
            notional / price
        )


    margin = (
        notional / LEVERAGE
    )


    # ========================================================
    # SL / TP
    # ========================================================

    if signal == "LONG":

        stop_loss = (
            price - stop_distance
        )

        take_profit = (
            price
            + atr * ATR_TAKE_MULTIPLIER
        )

    else:

        stop_loss = (
            price + stop_distance
        )

        take_profit = (
            price
            - atr * ATR_TAKE_MULTIPLIER
        )


    entry_fee = (
        notional * FEE_RATE
    )


    balance -= entry_fee


    positions[symbol] = {

        "symbol": symbol,

        "side": signal,

        "entry": price,

        "quantity": quantity,

        "notional": notional,

        "margin": margin,

        "stop_loss": stop_loss,

        "take_profit": take_profit,

        "entry_fee": entry_fee,

        "score": score,

        "opened_candle": data[
            "candle_time"
        ]

    }


    return True


# ============================================================
# POZİSYON KAPAT
# ============================================================

def close_position(
    symbol,
    exit_price,
    reason
):

    global balance


    position = positions[symbol]


    side = position["side"]

    entry = position["entry"]

    quantity = position["quantity"]


    if side == "LONG":

        gross_pnl = (
            exit_price - entry
        ) * quantity

    else:

        gross_pnl = (
            entry - exit_price
        ) * quantity


    exit_notional = (
        exit_price * quantity
    )


    exit_fee = (
        exit_notional * FEE_RATE
    )


    net_pnl = (
        gross_pnl
        - exit_fee
    )


    total_fees = (
        position["entry_fee"]
        + exit_fee
    )


    balance += net_pnl


    trade = {

        "time": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "symbol": symbol,

        "side": side,

        "entry": entry,

        "exit": exit_price,

        "quantity": quantity,

        "notional": position["notional"],

        "leverage": LEVERAGE,

        "score": position["score"],

        "gross_pnl": gross_pnl,

        "fees": total_fees,

        "net_pnl": net_pnl,

        "reason": reason,

        "balance": balance

    }


    trade_history.append(trade)

    save_trade(trade)


    del positions[symbol]


    return trade


# ============================================================
# POZİSYONLARI 15 DAKİKALIK MUM İLE KONTROL
# ============================================================

def check_position_with_candle(
    symbol,
    candle
):

    if symbol not in positions:
        return None


    position = positions[symbol]


    high = float(candle["high"])

    low = float(candle["low"])


    side = position["side"]


    # ========================================================
    # LONG
    # ========================================================

    if side == "LONG":

        hit_stop = (
            low <= position["stop_loss"]
        )

        hit_target = (
            high >= position["take_profit"]
        )


        # Aynı mumda ikisi de görülürse
        # konservatif olarak STOP kabul ediyoruz.
        if hit_stop:

            return close_position(
                symbol,
                position["stop_loss"],
                "STOP_LOSS"
            )


        if hit_target:

            return close_position(
                symbol,
                position["take_profit"],
                "TAKE_PROFIT"
            )


    # ========================================================
    # SHORT
    # ========================================================

    if side == "SHORT":

        hit_stop = (
            high >= position["stop_loss"]
        )

        hit_target = (
            low <= position["take_profit"]
        )


        if hit_stop:

            return close_position(
                symbol,
                position["stop_loss"],
                "STOP_LOSS"
            )


        if hit_target:

            return close_position(
                symbol,
                position["take_profit"],
                "TAKE_PROFIT"
            )


    return None


# ============================================================
# CSV
# ============================================================

def save_trade(trade):

    exists = os.path.exists(
        TRADE_FILE
    )


    pd.DataFrame(
        [trade]
    ).to_csv(
        TRADE_FILE,
        mode="a",
        header=not exists,
        index=False
    )


# ============================================================
# 15 DAKİKALIK RAPOR
# ============================================================

def print_report(
    start_balance,
    opened,
    closed
):

    global balance


    net_period = (
        balance - start_balance
    )


    net_percent = (
        net_period
        / start_balance
    ) * 100


    wins = [
        x for x in closed
        if x["net_pnl"] > 0
    ]


    losses = [
        x for x in closed
        if x["net_pnl"] < 0
    ]


    gross_profit = sum(
        x["net_pnl"]
        for x in wins
    )


    gross_loss = sum(
        x["net_pnl"]
        for x in losses
    )


    total_fees = sum(
        x["fees"]
        for x in closed
    )


    longs = sum(
        1 for x in opened
        if x["side"] == "LONG"
    )


    shorts = sum(
        1 for x in opened
        if x["side"] == "SHORT"
    )


    print("\n")

    print("=" * 72)

    print(
        "          15 DAKİKALIK PAPER TRADING RAPORU"
    )

    print("=" * 72)

    print(
        f"Başlangıç bakiye : "
        f"{start_balance:,.2f} USDT"
    )

    print(
        f"Güncel bakiye    : "
        f"{balance:,.2f} USDT"
    )

    print("-" * 72)

    print(
        f"Açılan işlem     : "
        f"{len(opened)}"
    )

    print(
        f"Kapanan işlem    : "
        f"{len(closed)}"
    )

    print(
        f"Açık pozisyon    : "
        f"{len(positions)}"
    )

    print(
        f"LONG             : "
        f"{longs}"
    )

    print(
        f"SHORT            : "
        f"{shorts}"
    )

    print("-" * 72)

    print(
        f"Kârlı işlem      : "
        f"{len(wins)}"
    )

    print(
        f"Zararlı işlem    : "
        f"{len(losses)}"
    )

    print(
        f"Brüt kâr         : "
        f"+{gross_profit:,.2f} USDT"
    )

    print(
        f"Brüt zarar       : "
        f"{gross_loss:,.2f} USDT"
    )

    print(
        f"Komisyon         : "
        f"-{total_fees:,.2f} USDT"
    )

    print("-" * 72)

    print(
        f"NET P/L          : "
        f"{net_period:+,.2f} USDT"
    )

    print(
        f"NET GETİRİ       : "
        f"{net_percent:+.2f}%"
    )

    print("=" * 72)

    print(
        "Bir sonraki 15 dakikalık periyot bekleniyor..."
    )


# ============================================================
# İLK BAŞLANGIÇ
# ============================================================

print("\n")

print("=" * 72)

print(
    "       15 DAKİKALIK FUTURES PAPER TRADING"
)

print("=" * 72)

print(
    f"Sanal bakiye : {START_BALANCE:,.2f} USDT"
)

print(
    f"Kaldıraç     : {LEVERAGE}x"
)

print(
    f"Risk/işlem   : {RISK_PER_TRADE * 100:.2f}%"
)

print(
    f"SL           : {ATR_STOP_MULTIPLIER} ATR"
)

print(
    f"TP           : {ATR_TAKE_MULTIPLIER} ATR"
)

print(
    f"Timeframe    : {TIMEFRAME}"
)

print(
    "Gerçek emir   : HAYIR"
)

print("=" * 72)


# ============================================================
# ANA DÖNGÜ
# ============================================================

last_processed_candle = None


try:

    while True:

        # ----------------------------------------------------
        # BTC zamanı / son mum
        # ----------------------------------------------------

        reference_symbol = "ETH/USDT"

        candles = exchange.fetch_ohlcv(
            reference_symbol,
            timeframe=TIMEFRAME,
            limit=3
        )


        if len(candles) < 2:

            time.sleep(10)

            continue


        completed_candle = candles[-2]


        candle_time = completed_candle[0]


        # Aynı mumu ikinci kez işleme
        if candle_time == last_processed_candle:

            time.sleep(10)

            continue


        last_processed_candle = candle_time


        candle_start_balance = balance

        opened_this_period = []

        closed_this_period = []


        print(
            "\n15 dakikalık mum kapandı."
        )


        # ----------------------------------------------------
        # Açık pozisyonları kontrol et
        # ----------------------------------------------------

        for symbol in list(
            positions.keys()
        ):

            try:

                candles_symbol = (
                    exchange.fetch_ohlcv(
                        symbol,
                        timeframe=TIMEFRAME,
                        limit=3
                    )
                )


                if len(candles_symbol) < 2:
                    continue


                candle = candles_symbol[-2]


                candle_data = {

                    "open": candle[1],

                    "high": candle[2],

                    "low": candle[3],

                    "close": candle[4]

                }


                result = (
                    check_position_with_candle(
                        symbol,
                        candle_data
                    )
                )


                if result:

                    closed_this_period.append(
                        result
                    )


            except Exception as error:

                print(
                    f"{symbol} pozisyon kontrolü "
                    f"hatası: {error}"
                )


        # ----------------------------------------------------
        # Yeni sinyalleri tara
        # ----------------------------------------------------

        candidates = []


        # Likiditeyi tek seferde al
        tickers = get_tickers()


        for symbol in symbols:

            if symbol in positions:
                continue


            ticker = tickers.get(symbol)


            if not ticker:
                continue


            volume = ticker.get(
                "quoteVolume"
            )


            if not volume:
                continue


            if volume < MIN_VOLUME_USDT:
                continue


            result = analyze_symbol(
                symbol
            )


            if not result:
                continue


            if result["signal"] in {
                "LONG",
                "SHORT"
            }:

                candidates.append(
                    result
                )


        candidates.sort(
            key=lambda x: x["score"],
            reverse=True
        )


        # ----------------------------------------------------
        # Pozisyonları aç
        # ----------------------------------------------------

        for candidate in candidates:

            if len(positions) >= MAX_POSITIONS:
                break


            success = open_position(
                candidate
            )


            if success:

                opened_this_period.append({

                    "symbol":
                        candidate["symbol"],

                    "side":
                        candidate["signal"]

                })


        # ----------------------------------------------------
        # RAPOR
        # ----------------------------------------------------

        print_report(
            candle_start_balance,
            opened_this_period,
            closed_this_period
        )


        # ----------------------------------------------------
        # Yeni mum bekle
        # ----------------------------------------------------

        while True:

            current = exchange.fetch_ohlcv(
                reference_symbol,
                timeframe=TIMEFRAME,
                limit=2
            )


            if len(current) < 2:

                time.sleep(10)

                continue


            new_completed = current[-2][0]


            if new_completed != last_processed_candle:

                break


            time.sleep(10)


except KeyboardInterrupt:

    print("\n")

    print(
        "Paper trading durduruldu."
    )

    print(
        f"Sanal bakiye: "
        f"{balance:,.2f} USDT"
    )

    print(
        f"Açık pozisyon: "
        f"{len(positions)}"
    )

    print(
        "Gerçek Binance emri gönderilmedi."
    )