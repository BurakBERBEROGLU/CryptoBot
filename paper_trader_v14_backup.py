import ccxt
import websocket
import threading
import json
import time
import csv
import os
from datetime import datetime


# ============================================================
# V14
# BINANCE FUTURES
# 1 DAKİKALIK KLINE + PAPER TRADING
# ============================================================

START_BALANCE = 1000.0

LEVERAGE = 5

MAX_POSITIONS = 10

RISK_PER_TRADE = 0.01

MAX_DAILY_LOSS = 0.05

FEE_RATE = 0.001


# ============================================================
# SL / TP
# ============================================================

STOP_LOSS_PERCENT = 0.0075
TAKE_PROFIT_PERCENT = 0.015


# ============================================================
# SCAN
# ============================================================

SCAN_INTERVAL = 60

DISPLAY_INTERVAL = 2

ENTRY_COOLDOWN_SECONDS = 60

TOP_CANDIDATES_TO_SHOW = 20


# ============================================================
# MARKET
# ============================================================

MIN_24H_VOLUME = 500_000

EXCLUDED_BASES = {
    "BTC",
    "USDC",
    "USDP",
    "FDUSD",
    "TUSD",
    "DAI",
    "USDE",
    "USDS",
    "USD1"
}


# ============================================================
# WEBSOCKET
# ============================================================

STREAMS_PER_CONNECTION = 200


# ============================================================
# CSV
# ============================================================

TRADE_LOG_FILE = "paper_trades.csv"


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
# GLOBAL
# ============================================================

running = True

symbols = []

ws_symbol_map = {}

market_data = {}

positions = {}

last_trade_time = {}

last_scan_time = 0

scan_number = 0

daily_start_equity = START_BALANCE

main_balance = START_BALANCE

reserved_margin = 0.0

stats = {
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "breakeven": 0,
    "gross_pnl": 0.0,
    "net_pnl": 0.0,
    "entry_fees": 0.0,
    "exit_fees": 0.0
}


# ============================================================
# SEMBOL
# ============================================================

def normalize_symbol(symbol):

    return (
        symbol
        .replace("/", "")
        .replace(":", "")
        .replace("-", "")
        .upper()
    )


# ============================================================
# CSV
# ============================================================

def initialize_trade_log():

    if os.path.exists(TRADE_LOG_FILE):
        return

    with open(
        TRADE_LOG_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(f)

        writer.writerow([
            "time",
            "symbol",
            "side",
            "entry_price",
            "exit_price",
            "quantity",
            "leverage",
            "margin",
            "notional",
            "entry_fee",
            "exit_fee",
            "gross_pnl",
            "net_pnl",
            "reason"
        ])


def log_trade(
    position,
    exit_price,
    exit_fee,
    gross_pnl,
    net_pnl,
    reason
):

    with open(
        TRADE_LOG_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(f)

        writer.writerow([
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            position["symbol"],
            position["side"],
            position["entry_price"],
            exit_price,
            position["quantity"],
            position["leverage"],
            position["margin"],
            position["notional"],
            position["entry_fee"],
            exit_fee,
            gross_pnl,
            net_pnl,
            reason
        ])


# ============================================================
# MARKETLER
# ============================================================

def load_symbols():

    global symbols
    global ws_symbol_map

    print()
    print("=" * 80)
    print("BINANCE FUTURES MARKETLERİ YÜKLENİYOR")
    print("=" * 80)

    markets = exchange.load_markets()

    selected = []

    for symbol, market in markets.items():

        try:

            if not market.get(
                "active",
                True
            ):
                continue

            if market.get("linear") is not True:
                continue

            if market.get("swap") is not True:
                continue

            if market.get("quote") != "USDT":
                continue

            base = market.get(
                "base",
                ""
            ).upper()

            if base in EXCLUDED_BASES:
                continue

            selected.append(symbol)

            raw = normalize_symbol(
                symbol
            )

            ws_symbol_map[
                raw
            ] = symbol

        except Exception:
            continue

    symbols = sorted(
        set(selected)
    )

    print(
        f"İzlenecek perpetual sayısı: "
        f"{len(symbols)}"
    )

    return symbols


# ============================================================
# MARKET DATA
# ============================================================

def initialize_market_data():

    for symbol in symbols:

        market_data[symbol] = {
            "price": 0.0,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0.0,
            "change": 0.0,
            "candle_open_time": 0,
            "candle_close_time": 0,
            "closed": False,
            "received": False,
            "updated": 0
        }


# ============================================================
# KLINE
# ============================================================

def process_kline(data):

    try:

        k = data.get("k")

        if not k:
            return

        raw_symbol = k.get("s")

        if not raw_symbol:
            return

        symbol = ws_symbol_map.get(
            raw_symbol.upper()
        )

        if not symbol:
            return

        open_price = float(
            k["o"]
        )

        high_price = float(
            k["h"]
        )

        low_price = float(
            k["l"]
        )

        close_price = float(
            k["c"]
        )

        volume = float(
            k["q"]
        )

        open_time = int(
            k["t"]
        )

        close_time = int(
            k["T"]
        )

        is_closed = bool(
            k["x"]
        )

        if open_price <= 0:
            return

        change = (
            (
                close_price
                - open_price
            )
            / open_price
        ) * 100

        market_data[symbol] = {
            "price": close_price,

            "open": open_price,

            "high": high_price,

            "low": low_price,

            "close": close_price,

            "volume": volume,

            "change": change,

            "candle_open_time": open_time,

            "candle_close_time": close_time,

            "closed": is_closed,

            "received": True,

            "updated": time.time()
        }

    except Exception:
        pass


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def on_message(
    ws,
    message
):

    try:

        payload = json.loads(
            message
        )

        # Binance combined stream
        data = payload.get(
            "data"
        )

        if data is None:

            data = payload

        event_type = data.get(
            "e"
        )

        if event_type == "kline":

            process_kline(
                data
            )

    except Exception:
        pass


def on_error(
    ws,
    error
):

    print(
        f"\n⚠️ WebSocket hata: {error}"
    )


def on_close(
    ws,
    code,
    msg
):

    print(
        "\n⚠️ WebSocket bağlantısı kapandı."
    )


def on_open(ws):

    print(
        "\n✅ Binance KLINE WebSocket bağlantısı açıldı."
    )


# ============================================================
# KLINE WEBSOCKET
# ============================================================

def create_streams():

    streams = []

    for symbol in symbols:

        raw = normalize_symbol(
            symbol
        ).lower()

        streams.append(
            f"{raw}@kline_1m"
        )

    return streams


def websocket_worker(
    streams
):

    global running

    while running:

        ws = None

        try:

            stream_string = "/".join(
                streams
            )

            url = (
                "wss://fstream.binance.com/"
                "stream?streams="
                + stream_string
            )

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            print(
                f"\n⚠️ WebSocket yeniden bağlanacak: {e}"
            )

        if running:

            time.sleep(3)


def start_websocket():

    streams = create_streams()

    print()
    print(
        f"{len(streams)} adet 1m KLINE stream hazırlanıyor."
    )

    chunks = []

    for i in range(
        0,
        len(streams),
        STREAMS_PER_CONNECTION
    ):

        chunks.append(
            streams[
                i:i + STREAMS_PER_CONNECTION
            ]
        )

    print(
        f"{len(chunks)} WebSocket bağlantısı kullanılacak."
    )

    for chunk in chunks:

        thread = threading.Thread(
            target=websocket_worker,
            args=(chunk,),
            daemon=True
        )

        thread.start()

        time.sleep(1)


# ============================================================
# 24H TICKER
# ============================================================

def update_24h_data():

    try:

        tickers = exchange.fetch_tickers()

        count = 0

        for symbol in symbols:

            ticker = tickers.get(
                symbol
            )

            if not ticker:
                continue

            volume = (
                ticker.get(
                    "quoteVolume"
                )
                or 0
            )

            last_price = (
                ticker.get(
                    "last"
                )
                or 0
            )

            market_data.setdefault(
                symbol,
                {}
            )

            market_data[symbol][
                "volume24h"
            ] = float(
                volume
            )

            if last_price:

                market_data[symbol][
                    "price"
                ] = float(
                    last_price
                )

            count += 1

        print(
            f"24H ticker güncellendi: "
            f"{count}/{len(symbols)}"
        )

    except Exception as e:

        print(
            f"⚠️ Ticker güncelleme hatası: {e}"
        )


# ============================================================
# HAZIR VERİ KONTROLÜ
# ============================================================

def get_ready_symbols():

    ready = []

    now = time.time()

    for symbol in symbols:

        data = market_data.get(
            symbol
        )

        if not data:
            continue

        if not data.get(
            "received"
        ):
            continue

        updated = data.get(
            "updated",
            0
        )

        if now - updated > 120:
            continue

        if data.get(
            "open",
            0
        ) <= 0:
            continue

        if data.get(
            "close",
            0
        ) <= 0:
            continue

        ready.append(
            symbol
        )

    return ready


# ============================================================
# ADAY SKORU
# ============================================================

def calculate_score(
    symbol
):

    data = market_data.get(
        symbol
    )

    if not data:
        return None

    change = float(
        data.get(
            "change",
            0
        )
    )

    volume24h = float(
        data.get(
            "volume24h",
            0
        )
    )

    candle_volume = float(
        data.get(
            "volume",
            0
        )
    )

    if change == 0:
        return None

    side = (
        "LONG"
        if change > 0
        else "SHORT"
    )

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    momentum_score = min(
        abs(change) * 50,
        70
    )

    # --------------------------------------------------------
    # HACİM
    # --------------------------------------------------------

    if volume24h >= MIN_24H_VOLUME:

        volume_score = 20

    else:

        volume_score = (
            volume24h
            / MIN_24H_VOLUME
        ) * 20

        volume_score = min(
            volume_score,
            20
        )

    # --------------------------------------------------------
    # MUM HACMİ
    # --------------------------------------------------------

    if candle_volume > 0:

        candle_score = 10

    else:

        candle_score = 0

    score = (
        momentum_score
        + volume_score
        + candle_score
    )

    return {
        "symbol": symbol,
        "side": side,
        "price": data["close"],
        "open": data["open"],
        "change": change,
        "volume24h": volume24h,
        "candle_volume": candle_volume,
        "score": min(
            score,
            100
        ),
        "candle_open_time":
            data["candle_open_time"],
        "candle_closed":
            data["closed"]
    }


# ============================================================
# TÜM ADAYLAR
# ============================================================

def get_candidates():

    candidates = []

    now = time.time()

    for symbol in symbols:

        if symbol in positions:
            continue

        last_entry = last_trade_time.get(
            symbol,
            0
        )

        if (
            now
            - last_entry
            < ENTRY_COOLDOWN_SECONDS
        ):
            continue

        result = calculate_score(
            symbol
        )

        if result is None:
            continue

        if result["price"] <= 0:
            continue

        result["age"] = (
            now
            - (
                result[
                    "candle_open_time"
                ] / 1000
            )
        )

        candidates.append(
            result
        )

    candidates.sort(
        key=lambda x: (
            x["score"],
            abs(x["change"])
        ),
        reverse=True
    )

    return candidates


# ============================================================
# OPEN PNL
# ============================================================

def calculate_open_pnl():

    total = 0.0

    for symbol, position in list(
        positions.items()
    ):

        current_price = (
            market_data
            .get(symbol, {})
            .get("price")
        )

        if not current_price:
            continue

        if position["side"] == "LONG":

            pnl = (
                current_price
                - position["entry_price"]
            ) * position["quantity"]

        else:

            pnl = (
                position["entry_price"]
                - current_price
            ) * position["quantity"]

        total += pnl

    return total


# ============================================================
# EQUITY
# ============================================================

def calculate_equity():

    return (
        main_balance
        + reserved_margin
        + calculate_open_pnl()
    )


# ============================================================
# DAILY LOSS
# ============================================================

def daily_loss_limit_hit():

    equity = calculate_equity()

    loss = (
        daily_start_equity
        - equity
    )

    limit = (
        daily_start_equity
        * MAX_DAILY_LOSS
    )

    return loss >= limit


# ============================================================
# POSITION SIZE
# ============================================================

def calculate_position_size():

    equity = calculate_equity()

    if equity <= 0:

        return None, "Equity <= 0"

    risk_amount = (
        equity
        * RISK_PER_TRADE
    )

    notional = (
        risk_amount
        / STOP_LOSS_PERCENT
    )

    available = main_balance

    max_notional = (
        available
        * LEVERAGE
        * 0.20
    )

    notional = min(
        notional,
        max_notional
    )

    if notional <= 0:

        return None, "Notional <= 0"

    margin = (
        notional
        / LEVERAGE
    )

    entry_fee = (
        notional
        * FEE_RATE
    )

    required = (
        margin
        + entry_fee
    )

    if required > available:

        return None, (
            f"Bakiye yetersiz: "
            f"gerekli={required:.2f}, "
            f"mevcut={available:.2f}"
        )

    return {
        "notional": notional,
        "margin": margin,
        "entry_fee": entry_fee
    }, None


# ============================================================
# OPEN POSITION
# ============================================================

def open_position(
    candidate
):

    global main_balance
    global reserved_margin

    print()
    print("=" * 90)
    print("POZİSYON AÇMA KONTROLÜ")
    print("=" * 90)

    symbol = candidate["symbol"]

    side = candidate["side"]

    entry_price = candidate["price"]

    print(
        f"Aday       : {symbol}"
    )

    print(
        f"Yön        : {side}"
    )

    print(
        f"1m hareket : "
        f"{candidate['change']:+.4f}%"
    )

    print(
        f"Skor       : "
        f"{candidate['score']:.2f}/100"
    )

    # --------------------------------------------------------
    # POSITION LIMIT
    # --------------------------------------------------------

    if len(positions) >= MAX_POSITIONS:

        print(
            "❌ POZİSYON AÇILMADI"
        )

        print(
            "Sebep: MAX_POSITIONS"
        )

        return False

    print(
        "✓ Pozisyon limiti uygun"
    )

    # --------------------------------------------------------
    # DAILY LOSS
    # --------------------------------------------------------

    if daily_loss_limit_hit():

        print(
            "❌ POZİSYON AÇILMADI"
        )

        print(
            "Sebep: Günlük zarar limiti"
        )

        return False

    print(
        "✓ Günlük zarar limiti uygun"
    )

    # --------------------------------------------------------
    # SIZE
    # --------------------------------------------------------

    size, error = (
        calculate_position_size()
    )

    if size is None:

        print(
            "❌ POZİSYON AÇILMADI"
        )

        print(
            f"Sebep: {error}"
        )

        return False

    print(
        "✓ Pozisyon büyüklüğü uygun"
    )

    notional = size[
        "notional"
    ]

    margin = size[
        "margin"
    ]

    entry_fee = size[
        "entry_fee"
    ]

    quantity = (
        notional
        / entry_price
    )

    if quantity <= 0:

        print(
            "❌ POZİSYON AÇILMADI"
        )

        print(
            "Sebep: quantity <= 0"
        )

        return False

    # --------------------------------------------------------
    # SL TP
    # --------------------------------------------------------

    if side == "LONG":

        stop_loss = (
            entry_price
            * (
                1
                - STOP_LOSS_PERCENT
            )
        )

        take_profit = (
            entry_price
            * (
                1
                + TAKE_PROFIT_PERCENT
            )
        )

    else:

        stop_loss = (
            entry_price
            * (
                1
                + STOP_LOSS_PERCENT
            )
        )

        take_profit = (
            entry_price
            * (
                1
                - TAKE_PROFIT_PERCENT
            )
        )

    required = (
        margin
        + entry_fee
    )

    if required > main_balance:

        print(
            "❌ POZİSYON AÇILMADI"
        )

        print(
            "Sebep: Bakiye yetersiz"
        )

        return False

    # --------------------------------------------------------
    # BALANCE
    # --------------------------------------------------------

    main_balance -= required

    reserved_margin += margin

    position = {

        "symbol": symbol,

        "side": side,

        "entry_price": entry_price,

        "quantity": quantity,

        "leverage": LEVERAGE,

        "margin": margin,

        "notional": notional,

        "entry_fee": entry_fee,

        "stop_loss": stop_loss,

        "take_profit": take_profit,

        "opened_at": time.time(),

        "score": candidate["score"],

        "momentum": candidate["change"]
    }

    positions[
        symbol
    ] = position

    last_trade_time[
        symbol
    ] = time.time()

    stats[
        "entry_fees"
    ] += entry_fee

    print()
    print("🚀🚀🚀 PAPER POZİSYON AÇILDI")
    print("=" * 90)

    print(
        f"Sembol       : {symbol}"
    )

    print(
        f"Yön          : {side}"
    )

    print(
        f"Giriş        : {entry_price:.8f}"
    )

    print(
        f"Notional     : {notional:.2f}"
    )

    print(
        f"Margin       : {margin:.2f}"
    )

    print(
        f"Entry Fee    : {entry_fee:.4f}"
    )

    print(
        f"SL           : {stop_loss:.8f}"
    )

    print(
        f"TP           : {take_profit:.8f}"
    )

    print(
        f"1m hareket   : "
        f"{candidate['change']:+.4f}%"
    )

    print(
        f"Skor         : "
        f"{candidate['score']:.2f}"
    )

    print("=" * 90)

    return True


# ============================================================
# CLOSE POSITION
# ============================================================

def close_position(
    symbol,
    exit_price,
    reason
):

    global main_balance
    global reserved_margin

    position = positions.get(
        symbol
    )

    if not position:
        return

    entry_price = position[
        "entry_price"
    ]

    quantity = position[
        "quantity"
    ]

    margin = position[
        "margin"
    ]

    entry_fee = position[
        "entry_fee"
    ]

    side = position[
        "side"
    ]

    if side == "LONG":

        gross_pnl = (
            exit_price
            - entry_price
        ) * quantity

    else:

        gross_pnl = (
            entry_price
            - exit_price
        ) * quantity

    exit_notional = (
        exit_price
        * quantity
    )

    exit_fee = (
        exit_notional
        * FEE_RATE
    )

    net_pnl = (
        gross_pnl
        - entry_fee
        - exit_fee
    )

    main_balance += (
        margin
        + gross_pnl
        - exit_fee
    )

    reserved_margin -= margin

    del positions[
        symbol
    ]

    stats[
        "total_trades"
    ] += 1

    stats[
        "gross_pnl"
    ] += gross_pnl

    stats[
        "net_pnl"
    ] += net_pnl

    stats[
        "exit_fees"
    ] += exit_fee

    if net_pnl > 0:

        stats[
            "wins"
        ] += 1

    elif net_pnl < 0:

        stats[
            "losses"
        ] += 1

    else:

        stats[
            "breakeven"
        ] += 1

    log_trade(
        position,
        exit_price,
        exit_fee,
        gross_pnl,
        net_pnl,
        reason
    )

    print()
    print("=" * 90)
    print("🔴 PAPER POZİSYON KAPANDI")
    print("=" * 90)

    print(
        f"Sembol    : {symbol}"
    )

    print(
        f"Yön       : {side}"
    )

    print(
        f"Giriş     : {entry_price:.8f}"
    )

    print(
        f"Çıkış     : {exit_price:.8f}"
    )

    print(
        f"Gross P/L : {gross_pnl:+.4f}"
    )

    print(
        f"Entry Fee : {entry_fee:.4f}"
    )

    print(
        f"Exit Fee  : {exit_fee:.4f}"
    )

    print(
        f"Net P/L   : {net_pnl:+.4f}"
    )

    print(
        f"Neden     : {reason}"
    )

    print("=" * 90)


# ============================================================
# POSITION MONITOR
# ============================================================

def monitor_positions():

    for symbol, position in list(
        positions.items()
    ):

        current_price = (
            market_data
            .get(symbol, {})
            .get("price")
        )

        if not current_price:
            continue

        if position["side"] == "LONG":

            if (
                current_price
                <= position["stop_loss"]
            ):

                close_position(
                    symbol,
                    current_price,
                    "STOP LOSS"
                )

            elif (
                current_price
                >= position["take_profit"]
            ):

                close_position(
                    symbol,
                    current_price,
                    "TAKE PROFIT"
                )

        else:

            if (
                current_price
                >= position["stop_loss"]
            ):

                close_position(
                    symbol,
                    current_price,
                    "STOP LOSS"
                )

            elif (
                current_price
                <= position["take_profit"]
            ):

                close_position(
                    symbol,
                    current_price,
                    "TAKE PROFIT"
                )


# ============================================================
# İSTATİSTİK
# ============================================================

def print_statistics():

    total = stats[
        "total_trades"
    ]

    if total > 0:

        win_rate = (
            stats["wins"]
            / total
        ) * 100

    else:

        win_rate = 0

    print(
        f"İşlem:{total} | "
        f"Win:{stats['wins']} | "
        f"Loss:{stats['losses']} | "
        f"WinRate:%{win_rate:.1f} | "
        f"Net:{stats['net_pnl']:+.4f}"
    )


# ============================================================
# DISPLAY
# ============================================================

def display_status(
    candidates
):

    print(
        "\033[2J\033[H"
    )

    equity = calculate_equity()

    open_pnl = calculate_open_pnl()

    ready = get_ready_symbols()

    print("=" * 110)

    print(
        "BINANCE FUTURES PAPER TRADER V14"
    )

    print(
        "1 DAKİKALIK KLINE MOMENTUM"
    )

    print("=" * 110)

    print(
        f"Bakiye       : "
        f"{main_balance:.2f} USDT"
    )

    print(
        f"Reserved     : "
        f"{reserved_margin:.2f} USDT"
    )

    print(
        f"Açık P/L     : "
        f"{open_pnl:+.4f} USDT"
    )

    print(
        f"Equity       : "
        f"{equity:.2f} USDT"
    )

    print(
        f"Pozisyon     : "
        f"{len(positions)}/{MAX_POSITIONS}"
    )

    print(
        f"KLINE veri   : "
        f"{len(ready)}/{len(symbols)}"
    )

    print(
        f"Tarama       : "
        f"#{scan_number}"
    )

    print("-" * 110)

    print(
        "AÇIK POZİSYONLAR"
    )

    print("-" * 110)

    if positions:

        for symbol, position in positions.items():

            price = (
                market_data
                .get(symbol, {})
                .get(
                    "price",
                    position[
                        "entry_price"
                    ]
                )
            )

            if position["side"] == "LONG":

                pnl = (
                    price
                    - position[
                        "entry_price"
                    ]
                ) * position[
                    "quantity"
                ]

            else:

                pnl = (
                    position[
                        "entry_price"
                    ]
                    - price
                ) * position[
                    "quantity"
                ]

            print(
                f"{symbol:<22} "
                f"{position['side']:<5} "
                f"Giriş:{position['entry_price']:<14.8f} "
                f"Fiyat:{price:<14.8f} "
                f"P/L:{pnl:+.4f}"
            )

    else:

        print(
            "Açık pozisyon yok."
        )

    print("-" * 110)

    print(
        "SON 1 DAKİKALIK ADAYLAR"
    )

    print("-" * 110)

    if candidates:

        for candidate in candidates[
            :TOP_CANDIDATES_TO_SHOW
        ]:

            print(
                f"{candidate['symbol']:<22} "
                f"{candidate['side']:<5} "
                f"1m:{candidate['change']:+.4f}% "
                f"Skor:{candidate['score']:6.2f} "
                f"24hVol:"
                f"{candidate.get('volume24h', 0)/1_000_000:7.2f}M"
            )

    else:

        print(
            "Aday yok."
        )

    print("-" * 110)

    print_statistics()

    print("-" * 110)

    print(
        "PAPER TRADING - GERÇEK EMİR YOK"
    )


# ============================================================
# TARAMA
# ============================================================

def scan_and_trade():

    global last_scan_time
    global scan_number

    now = time.time()

    if (
        now
        - last_scan_time
        < SCAN_INTERVAL
    ):

        return []

    last_scan_time = now

    scan_number += 1

    print()
    print()
    print("#" * 100)

    print(
        f"1 DAKİKALIK TARAMA #{scan_number}"
    )

    print("#" * 100)

    ready = get_ready_symbols()

    print(
        f"KLINE verisi hazır: "
        f"{len(ready)}/{len(symbols)}"
    )

    if len(ready) == 0:

        print()
        print(
            "❌ HİÇ KLINE VERİSİ YOK."
        )

        print(
            "WebSocket bağlantısı kontrol edilmeli."
        )

        return []

    update_24h_data()

    candidates = get_candidates()

    print(
        f"Aday sayısı: "
        f"{len(candidates)}"
    )

    if not candidates:

        print(
            "⚠️ Aday bulunamadı."
        )

        return []

    print()
    print(
        "EN GÜÇLÜ 5 ADAY:"
    )

    for i, candidate in enumerate(
        candidates[:5],
        start=1
    ):

        print(
            f"{i}. "
            f"{candidate['symbol']} "
            f"{candidate['side']} | "
            f"1m:"
            f"{candidate['change']:+.4f}% | "
            f"Skor:"
            f"{candidate['score']:.2f}"
        )

    if len(positions) >= MAX_POSITIONS:

        print()
        print(
            "❌ Pozisyon açılmadı: "
            "MAX_POSITIONS"
        )

        return candidates

    if daily_loss_limit_hit():

        print()
        print(
            "❌ Pozisyon açılmadı: "
            "Günlük zarar limiti"
        )

        return candidates

    best = candidates[0]

    print()
    print(
        "🎯 SEÇİLEN ADAY"
    )

    print(
        f"{best['symbol']} "
        f"{best['side']}"
    )

    print(
        f"1m hareket: "
        f"{best['change']:+.4f}%"
    )

    print(
        f"Skor: "
        f"{best['score']:.2f}/100"
    )

    open_position(
        best
    )

    return candidates


# ============================================================
# MAIN
# ============================================================

def main():

    global running
    global daily_start_equity

    initialize_trade_log()

    load_symbols()

    initialize_market_data()

    start_websocket()

    print()
    print(
        "KLINE WebSocket verileri bekleniyor..."
    )

    start_wait = time.time()

    while (
        time.time()
        - start_wait
        < 20
    ):

        ready = len(
            get_ready_symbols()
        )

        print(
            f"\rGelen KLINE verisi: "
            f"{ready}/{len(symbols)}",
            end="",
            flush=True
        )

        if ready > 0:
            break

        time.sleep(1)

    print()

    # İlk ticker
    update_24h_data()

    daily_start_equity = (
        calculate_equity()
    )

    print()
    print(
        "=" * 80
    )

    print(
        "V14 PAPER TRADER BAŞLADI"
    )

    print(
        "Gerçek Binance emri gönderilmez."
    )

    print(
        "Her 60 saniyede 1m KLINE taranır."
    )

    print(
        "=" * 80
    )

    last_display = 0

    latest_candidates = []

    try:

        while True:

            monitor_positions()

            result = scan_and_trade()

            if result:

                latest_candidates = result

            now = time.time()

            if (
                now
                - last_display
                >= DISPLAY_INTERVAL
            ):

                last_display = now

                display_status(
                    latest_candidates
                )

            time.sleep(
                0.2
            )

    except KeyboardInterrupt:

        running = False

        print()
        print(
            "V14 durduruldu."
        )

        print_statistics()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()