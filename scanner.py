import ccxt
import pandas as pd

from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange


# ============================================================
# AYARLAR
# ============================================================

TIMEFRAME = "15m"

CANDLE_LIMIT = 150

MIN_VOLUME_USDT = 1_000_000

MAX_CANDIDATES = 20

MIN_SCORE = 60


# ============================================================
# BINANCE USDT-M FUTURES
# ============================================================

exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {
        "defaultType": "future"
    }
})


print("\nBinance USDT-M Futures piyasaları yükleniyor...")


# ============================================================
# MARKETLER
# ============================================================

markets = exchange.load_markets()


symbols = []

excluded = {
    "BTC",
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "DAI"
}


for symbol, market in markets.items():

    if not market.get("active"):
        continue

    if market.get("swap") is not True:
        continue

    if market.get("quote") != "USDT":
        continue

    base = market.get("base")

    if base in excluded:
        continue

    symbols.append(symbol)


print(
    f"\nFutures altcoin sayısı: {len(symbols)}"
)


# ============================================================
# 24 SAATLİK TICKER VERİLERİ
# ============================================================

print("\nPiyasa verileri alınıyor...")

tickers = exchange.fetch_tickers(symbols)


# ============================================================
# ÖN FİLTRE
# ============================================================

liquid_symbols = []


for symbol in symbols:

    ticker = tickers.get(symbol)

    if not ticker:
        continue

    volume = ticker.get("quoteVolume")

    if not volume:
        continue

    if volume < MIN_VOLUME_USDT:
        continue

    liquid_symbols.append(symbol)


print(
    f"Likidite filtresinden geçen coin: "
    f"{len(liquid_symbols)}"
)


# ============================================================
# ANALİZ
# ============================================================

results = []


for index, symbol in enumerate(liquid_symbols, start=1):

    print(
        f"[{index}/{len(liquid_symbols)}] "
        f"{symbol} analiz ediliyor..."
    )

    try:

        ticker = tickers.get(symbol)

        price = ticker.get("last")

        change_24h = ticker.get("percentage")

        volume_24h = ticker.get("quoteVolume")


        if not price:
            continue


        # ----------------------------------------------------
        # OHLCV
        # ----------------------------------------------------

        ohlcv = exchange.fetch_ohlcv(
            symbol,
            timeframe=TIMEFRAME,
            limit=CANDLE_LIMIT
        )


        if len(ohlcv) < 60:
            continue


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


        # ----------------------------------------------------
        # EMA
        # ----------------------------------------------------

        df["EMA20"] = EMAIndicator(
            close=df["close"],
            window=20
        ).ema_indicator()


        df["EMA50"] = EMAIndicator(
            close=df["close"],
            window=50
        ).ema_indicator()


        # ----------------------------------------------------
        # RSI
        # ----------------------------------------------------

        df["RSI"] = RSIIndicator(
            close=df["close"],
            window=14
        ).rsi()


        # ----------------------------------------------------
        # ATR
        # ----------------------------------------------------

        atr_indicator = AverageTrueRange(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=14
        )

        df["ATR"] = atr_indicator.average_true_range()


        # ----------------------------------------------------
        # HACİM ORANI
        # ----------------------------------------------------

        df["volume_avg"] = (
            df["volume"]
            .rolling(20)
            .mean()
        )


        df["volume_ratio"] = (
            df["volume"]
            / df["volume_avg"]
        )


        # Son mum
        last = df.iloc[-1]


        ema20 = last["EMA20"]
        ema50 = last["EMA50"]

        rsi = last["RSI"]

        atr = last["ATR"]

        volume_ratio = last["volume_ratio"]


        # ====================================================
        # LONG SKORU
        # ====================================================

        long_score = 0


        # Trend
        if ema20 > ema50:
            long_score += 30


        # RSI
        if 45 <= rsi <= 65:
            long_score += 20

        elif 40 <= rsi < 45:
            long_score += 10


        # Hacim
        if volume_ratio >= 1.5:
            long_score += 20

        elif volume_ratio >= 1.2:
            long_score += 10


        # 24 saat momentum
        if change_24h is not None:

            if change_24h >= 5:
                long_score += 20

            elif change_24h >= 2:
                long_score += 10


        # Fiyatın EMA20 üzerinde olması
        if price > ema20:
            long_score += 10


        # ====================================================
        # SHORT SKORU
        # ====================================================

        short_score = 0


        # Trend
        if ema20 < ema50:
            short_score += 30


        # RSI
        if 35 <= rsi <= 55:
            short_score += 20

        elif 55 < rsi <= 60:
            short_score += 10


        # Hacim
        if volume_ratio >= 1.5:
            short_score += 20

        elif volume_ratio >= 1.2:
            short_score += 10


        # 24 saat momentum
        if change_24h is not None:

            if change_24h <= -5:
                short_score += 20

            elif change_24h <= -2:
                short_score += 10


        # Fiyat EMA20 altında
        if price < ema20:
            short_score += 10


        # ====================================================
        # SİNYAL
        # ====================================================

        if long_score >= MIN_SCORE and long_score > short_score:

            signal = "LONG"
            score = long_score


        elif short_score >= MIN_SCORE and short_score > long_score:

            signal = "SHORT"
            score = short_score


        else:

            signal = "BEKLE"
            score = max(
                long_score,
                short_score
            )


        # ====================================================
        # ATR YÜZDESİ
        # ====================================================

        atr_percent = (
            atr / price
        ) * 100


        results.append({

            "symbol": symbol,

            "price": price,

            "signal": signal,

            "score": score,

            "long_score": long_score,

            "short_score": short_score,

            "RSI": rsi,

            "EMA20": ema20,

            "EMA50": ema50,

            "ATR": atr,

            "ATR_percent": atr_percent,

            "volume_ratio": volume_ratio,

            "change_24h": change_24h,

            "volume_24h": volume_24h

        })


    except Exception as error:

        print(
            f"{symbol} analiz edilemedi: "
            f"{error}"
        )


# ============================================================
# SONUÇLAR
# ============================================================

results_df = pd.DataFrame(results)


if results_df.empty:

    print("\nAnaliz sonucu bulunamadı.")

    exit()


# ============================================================
# GÜÇLÜ SİNYALLER
# ============================================================

signals = results_df[
    results_df["signal"] != "BEKLE"
].copy()


signals = signals.sort_values(
    by="score",
    ascending=False
)


# ============================================================
# BAŞLIK
# ============================================================

print("\n")
print("=" * 110)

print(
    "             GELİŞMİŞ FUTURES SİNYAL MOTORU"
)

print("=" * 110)


print(
    f"Taranan coin       : {len(results_df)}"
)

print(
    f"İşlem adayı        : {len(signals)}"
)


print("=" * 110)


# ============================================================
# SONUÇ
# ============================================================

print("\nGÜÇLÜ FUTURES ADAYLARI:\n")


for _, row in signals.head(MAX_CANDIDATES).iterrows():

    print(
        f"{row['symbol']:18}"
        f" {row['signal']:6}"
        f" SKOR: {row['score']:3.0f}/100"
        f" RSI: {row['RSI']:6.2f}"
        f" 24s: {row['change_24h']:7.2f}%"
        f" VOL: {row['volume_ratio']:5.2f}x"
        f" ATR: {row['ATR_percent']:5.2f}%"
    )


# ============================================================
# LONG
# ============================================================

print("\n")
print("=" * 110)

print("LONG ADAYLARI")

print("=" * 110)


longs = signals[
    signals["signal"] == "LONG"
]


for _, row in longs.head(10).iterrows():

    print(
        f"{row['symbol']:18}"
        f" SKOR: {row['score']:3.0f}"
        f" RSI: {row['RSI']:6.2f}"
        f" 24s: {row['change_24h']:7.2f}%"
        f" VOL: {row['volume_ratio']:5.2f}x"
    )


# ============================================================
# SHORT
# ============================================================

print("\n")
print("=" * 110)

print("SHORT ADAYLARI")

print("=" * 110)


shorts = signals[
    signals["signal"] == "SHORT"
]


for _, row in shorts.head(10).iterrows():

    print(
        f"{row['symbol']:18}"
        f" SKOR: {row['score']:3.0f}"
        f" RSI: {row['RSI']:6.2f}"
        f" 24s: {row['change_24h']:7.2f}%"
        f" VOL: {row['volume_ratio']:5.2f}x"
    )


print("\nTarama tamamlandı.")