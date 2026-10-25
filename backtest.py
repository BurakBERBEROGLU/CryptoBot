import ccxt
import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator


# ============================================================
# AYARLAR
# ============================================================

SYMBOL = "BTC/USDT"
TIMEFRAME = "15m"

INITIAL_BALANCE = 10_000.0

POSITION_SIZE = 0.95

FEE_RATE = 0.001

STOP_LOSS_PERCENT = 0.02
TAKE_PROFIT_PERCENT = 0.04


# ============================================================
# BORSA
# ============================================================

exchange = ccxt.binance()

print("\nVeriler alınıyor...")

ohlcv = exchange.fetch_ohlcv(
    SYMBOL,
    timeframe=TIMEFRAME,
    limit=1000
)


# ============================================================
# DATAFRAME
# ============================================================

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

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    unit="ms"
)


# ============================================================
# İNDİKATÖRLER
# ============================================================

df["EMA20"] = EMAIndicator(
    close=df["close"],
    window=20
).ema_indicator()

df["EMA50"] = EMAIndicator(
    close=df["close"],
    window=50
).ema_indicator()

df["RSI"] = RSIIndicator(
    close=df["close"],
    window=14
).rsi()

df = df.dropna().reset_index(drop=True)


# ============================================================
# BACKTEST DEĞİŞKENLERİ
# ============================================================

balance = INITIAL_BALANCE

btc_amount = 0.0

in_position = False

entry_price = 0.0

stop_loss_price = 0.0

take_profit_price = 0.0

trades = []

equity_curve = []


# ============================================================
# BACKTEST
# ============================================================

for i in range(len(df)):

    row = df.iloc[i]

    open_price = row["open"]
    high_price = row["high"]
    low_price = row["low"]
    close_price = row["close"]

    # --------------------------------------------------------
    # POZİSYON YOKSA ALIŞ SİNYALİ
    # --------------------------------------------------------

    if not in_position:

        buy_signal = (
            row["EMA20"] > row["EMA50"]
            and row["RSI"] < 70
        )

        if buy_signal:

            investment = balance * POSITION_SIZE

            fee = investment * FEE_RATE

            btc_amount = (
                investment - fee
            ) / close_price

            balance -= investment

            entry_price = close_price

            stop_loss_price = (
                entry_price
                * (1 - STOP_LOSS_PERCENT)
            )

            take_profit_price = (
                entry_price
                * (1 + TAKE_PROFIT_PERCENT)
            )

            in_position = True

            trades.append({
                "type": "BUY",
                "time": row["timestamp"],
                "price": entry_price
            })


    # --------------------------------------------------------
    # POZİSYON AÇIK
    # --------------------------------------------------------

    else:

        exit_reason = None

        exit_price = None

        # Önce stop-loss kontrolü
        if low_price <= stop_loss_price:

            exit_price = stop_loss_price

            exit_reason = "STOP LOSS"

        # Daha sonra take-profit kontrolü
        elif high_price >= take_profit_price:

            exit_price = take_profit_price

            exit_reason = "TAKE PROFIT"

        else:

            sell_signal = (
                row["EMA20"] < row["EMA50"]
                and row["RSI"] > 30
            )

            if sell_signal:

                exit_price = close_price

                exit_reason = "SIGNAL"


        # ----------------------------------------------------
        # SATIŞ
        # ----------------------------------------------------

        if exit_price is not None:

            gross_value = (
                btc_amount
                * exit_price
            )

            fee = (
                gross_value
                * FEE_RATE
            )

            net_value = (
                gross_value
                - fee
            )

            original_value = (
                btc_amount
                * entry_price
            )

            profit = (
                net_value
                - original_value
            )

            balance += net_value

            trades.append({
                "type": "SELL",
                "time": row["timestamp"],
                "price": exit_price,
                "profit": profit,
                "reason": exit_reason
            })

            btc_amount = 0.0

            in_position = False

            entry_price = 0.0

            stop_loss_price = 0.0

            take_profit_price = 0.0


    # --------------------------------------------------------
    # PORTFÖY DEĞERİ
    # --------------------------------------------------------

    if in_position:

        portfolio_value = (
            balance
            + btc_amount * close_price
        )

    else:

        portfolio_value = balance

    equity_curve.append(
        portfolio_value
    )


# ============================================================
# AÇIK POZİSYONU KAPAT
# ============================================================

if in_position:

    final_price = df.iloc[-1]["close"]

    gross_value = (
        btc_amount
        * final_price
    )

    fee = (
        gross_value
        * FEE_RATE
    )

    net_value = (
        gross_value
        - fee
    )

    original_value = (
        btc_amount
        * entry_price
    )

    profit = (
        net_value
        - original_value
    )

    balance += net_value

    trades.append({
        "type": "SELL",
        "time": df.iloc[-1]["timestamp"],
        "price": final_price,
        "profit": profit,
        "reason": "END OF TEST"
    })


# ============================================================
# PERFORMANS
# ============================================================

final_balance = balance

net_profit = (
    final_balance
    - INITIAL_BALANCE
)

return_percentage = (
    net_profit
    / INITIAL_BALANCE
) * 100


# ============================================================
# İŞLEMLER
# ============================================================

completed_trades = [
    trade
    for trade in trades
    if trade["type"] == "SELL"
]

winning_trades = [
    trade
    for trade in completed_trades
    if trade["profit"] > 0
]

losing_trades = [
    trade
    for trade in completed_trades
    if trade["profit"] <= 0
]


if completed_trades:

    win_rate = (
        len(winning_trades)
        / len(completed_trades)
    ) * 100

else:

    win_rate = 0


# ============================================================
# PROFIT FACTOR
# ============================================================

gross_profit = sum(
    trade["profit"]
    for trade in winning_trades
)

gross_loss = abs(
    sum(
        trade["profit"]
        for trade in losing_trades
    )
)

if gross_loss > 0:

    profit_factor = (
        gross_profit
        / gross_loss
    )

else:

    profit_factor = float("inf")


# ============================================================
# MAX DRAWDOWN
# ============================================================

equity_series = pd.Series(
    equity_curve
)

running_max = (
    equity_series.cummax()
)

drawdown = (
    equity_series
    - running_max
) / running_max

max_drawdown = (
    drawdown.min()
    * 100
)


# ============================================================
# RAPOR
# ============================================================

print("\n")
print("=" * 55)
print("             CRYPTO BOT BACKTEST V2")
print("=" * 55)

print(f"Parite              : {SYMBOL}")
print(f"Zaman dilimi        : {TIMEFRAME}")

print("-" * 55)

print(
    f"Başlangıç sermayesi : "
    f"{INITIAL_BALANCE:,.2f} USDT"
)

print(
    f"Son sermaye         : "
    f"{final_balance:,.2f} USDT"
)

print(
    f"Net kâr/zarar       : "
    f"{net_profit:,.2f} USDT"
)

print(
    f"Getiri              : "
    f"{return_percentage:.2f}%"
)

print("-" * 55)

print(
    f"Stop-loss           : "
    f"{STOP_LOSS_PERCENT * 100:.1f}%"
)

print(
    f"Take-profit         : "
    f"{TAKE_PROFIT_PERCENT * 100:.1f}%"
)

print(
    f"Komisyon            : "
    f"{FEE_RATE * 100:.2f}%"
)

print("-" * 55)

print(
    f"Tamamlanan işlem    : "
    f"{len(completed_trades)}"
)

print(
    f"Kazanan işlem       : "
    f"{len(winning_trades)}"
)

print(
    f"Kaybeden işlem      : "
    f"{len(losing_trades)}"
)

print(
    f"Kazanma oranı       : "
    f"{win_rate:.2f}%"
)

print(
    f"Profit Factor       : "
    f"{profit_factor:.2f}"
)

print(
    f"Maksimum düşüş      : "
    f"{max_drawdown:.2f}%"
)

print("=" * 55)


# ============================================================
# İŞLEM GEÇMİŞİ
# ============================================================

print("\nSon işlemler:")

for trade in trades[-10:]:

    if trade["type"] == "BUY":

        print(
            f"BUY  | "
            f"{trade['time']} | "
            f"{trade['price']:.2f}"
        )

    else:

        print(
            f"SELL | "
            f"{trade['time']} | "
            f"{trade['price']:.2f} | "
            f"K/Z: {trade['profit']:.2f} | "
            f"{trade['reason']}"
        )