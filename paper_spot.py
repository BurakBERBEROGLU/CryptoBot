import csv
import json
import logging
import math
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import ccxt
import websocket


START_BALANCE = 200.0
POSITION_VALUE = 20.0
TAKE_PROFIT_USD = 2.0
STOP_LOSS_USD = 1.0
MAX_HOLDING_SECONDS = 60 * 60
FEE_RATE = 0.0005
SLIPPAGE_RATE = 0.0008
MAX_OPEN_POSITIONS = 10

MIN_VOLUME_24H = 3_000_000.0
MAX_UNIVERSE = 30
UNIVERSE_REFRESH_SECONDS = 900
ANALYSIS_INTERVAL_SECONDS = 60
PRICE_CHECK_INTERVAL_SECONDS = 5
ANALYSIS_WINDOW_SECONDS = 60
MIN_SCORE = 70.0
MIN_TRADES_60S = 20
MIN_MOMENTUM_PCT = 0.15
TRADE_HISTORY_LIMIT = 3000
STREAMS_PER_CONNECTION = 100
HTML_HOST = "127.0.0.1"
HTML_PORT = int(os.getenv("SPOT_BOT_PORT", "8080"))

TRADE_LOG_FILE = Path("spot_momentum_trades.csv")
STATE_FILE = Path("spot_momentum_state.json")
CSV_FIELDS = [
    "time", "trade_id", "symbol", "side", "trade_type", "entry_price",
    "exit_price", "stop_price", "tp_price", "quantity", "position_value",
    "gross_pnl", "entry_fee", "exit_fee", "net_pnl", "close_reason",
    "hold_seconds", "risk_usd", "score", "momentum_60s", "volume_60s",
    "trades_60s", "pressure",
]
EXCLUDED_BASES = {
    "BTC", "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI",
    "USDE", "USDS", "USD1",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spot_bot")

exchange = ccxt.binance({
    "enableRateLimit": True,
    "options": {"defaultType": "spot"},
})
lock = threading.RLock()
ws_lock = threading.Lock()

main_balance = START_BALANCE
positions = {}
universe = []
volume_by_symbol = {}
change_by_symbol = {}
trade_history = {}
raw_to_unified = {}
unified_to_raw = {}
ws_started = set()
last_candidates = []
recent_closed = []
last_analysis_time = 0.0
last_universe_update = 0.0
trade_id_counter = 0
running = True
stats = {
    "opened": 0,
    "closed": 0,
    "wins": 0,
    "losses": 0,
    "realized_pnl": 0.0,
    "total_fees": 0.0,
}


def calculate_spot_quantity(symbol, entry, available_balance):
    if min(entry, available_balance) <= 0:
        return 0.0

    if available_balance < POSITION_VALUE * (1.0 + FEE_RATE):
        return 0.0

    formatted_quantity = exchange.amount_to_precision(symbol, POSITION_VALUE / entry)
    quantity = float(formatted_quantity)
    if quantity * entry > POSITION_VALUE:
        decimal_places = len(formatted_quantity.partition(".")[2])
        amount_step = 10.0 ** -decimal_places
        quantity = float(exchange.amount_to_precision(symbol, quantity - amount_step))
    if quantity <= 0 or quantity * entry * (1.0 + FEE_RATE) > available_balance:
        return 0.0
    return quantity


def calculate_exit_price(position, target_net_pnl):
    quantity = float(position["quantity"])
    entry = float(position["entry_price"])
    entry_fee = float(position["entry_fee"])
    if quantity <= 0 or entry <= 0:
        raise ValueError("Pozisyon miktarı ve giriş fiyatı pozitif olmalıdır.")
    proceeds_factor = (1.0 - SLIPPAGE_RATE) * (1.0 - FEE_RATE)
    return (entry + (target_net_pnl + entry_fee) / quantity) / proceeds_factor


def get_60s_stats(symbol, now=None):
    current_time = time.time() if now is None else now
    cutoff = current_time - ANALYSIS_WINDOW_SECONDS
    with lock:
        history = trade_history.get(symbol)
        if not history:
            return None
        rows = [trade for trade in history if trade[0] >= cutoff]

    if len(rows) < MIN_TRADES_60S:
        return None

    first_price = rows[0][1]
    last_price = rows[-1][1]
    if first_price <= 0:
        return None

    momentum_pct = (last_price / first_price - 1.0) * 100.0
    volume_60s = sum(price * quantity for _, price, quantity, _ in rows)
    buy_volume = sum(
        price * quantity for _, price, quantity, buyer_maker in rows if not buyer_maker
    )
    buy_pressure = buy_volume / volume_60s if volume_60s > 0 else 0.5
    volume_24h = volume_by_symbol.get(symbol, 0.0)
    average_minute_volume = volume_24h / 1440.0 if volume_24h > 0 else 0.0
    activity_ratio = volume_60s / average_minute_volume if average_minute_volume > 0 else 0.0
    side = "LONG" if momentum_pct > 0 else "SHORT"
    momentum_score = min(abs(momentum_pct) / 1.0 * 40.0, 40.0)
    activity_score = min(activity_ratio / 5.0 * 30.0, 30.0)
    trade_score = min(len(rows) / 150.0 * 15.0, 15.0)

    if side == "LONG":
        pressure_score = max(0.0, min((buy_pressure - 0.50) / 0.25 * 15.0, 15.0))
        aligned = buy_pressure >= 0.50
    else:
        pressure_score = max(0.0, min((0.50 - buy_pressure) / 0.25 * 15.0, 15.0))
        aligned = buy_pressure <= 0.50

    return {
        "symbol": symbol,
        "side": side,
        "price": last_price,
        "momentum": momentum_pct,
        "volume_60s": volume_60s,
        "trades": len(rows),
        "buy_pressure": buy_pressure,
        "activity_ratio": activity_ratio,
        "score": momentum_score + activity_score + trade_score + pressure_score,
        "aligned": aligned,
        "volume_24h": volume_24h,
    }


def process_trade(raw_symbol, price, quantity, timestamp_ms, buyer_maker):
    symbol = raw_to_unified.get(raw_symbol)
    if not symbol or symbol not in universe:
        return
    if not all(math.isfinite(value) and value > 0 for value in (price, quantity, timestamp_ms)):
        return

    timestamp = timestamp_ms / 1000.0
    with lock:
        history = trade_history.setdefault(symbol, deque(maxlen=TRADE_HISTORY_LIMIT))
        history.append((timestamp, price, quantity, buyer_maker))
        cutoff = timestamp - ANALYSIS_WINDOW_SECONDS
        while history and history[0][0] < cutoff:
            history.popleft()

        position = positions.get(symbol)
        if position:
            position["current_price"] = price
            check_position_tick(position, price)


def websocket_worker(raw_symbols, connection_no):
    backoff = 1.0
    while running:
        ws = None
        try:
            ws = websocket.WebSocket()
            ws.settimeout(65)
            ws.connect("wss://stream.binance.com:9443/ws", timeout=30)
            ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@trade" for symbol in raw_symbols],
                "id": connection_no,
            }))
            backoff = 1.0
            logger.info(
                "Spot WebSocket #%s bağlandı; %s coin akışı dinleniyor",
                connection_no,
                len(raw_symbols),
            )

            while running:
                try:
                    message = ws.recv()
                except websocket.WebSocketTimeoutException:
                    ws.ping()
                    continue
                if not message:
                    continue
                data = json.loads(message)
                if data.get("e") != "trade":
                    continue
                process_trade(
                    str(data.get("s", "")).upper(),
                    float(data.get("p") or 0.0),
                    float(data.get("q") or 0.0),
                    float(data.get("T") or 0.0),
                    bool(data.get("m", False)),
                )
        except (websocket.WebSocketException, OSError, ValueError, TypeError) as error:
            if running:
                logger.warning("Spot WebSocket bağlantısı %s koptu: %s", connection_no, error)
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 20.0)
        finally:
            if ws is not None:
                ws.close()


def start_websockets_for(symbols):
    new_raw_symbols = []
    with ws_lock:
        for symbol in symbols:
            raw_symbol = unified_to_raw.get(symbol)
            if raw_symbol and raw_symbol not in ws_started:
                ws_started.add(raw_symbol)
                new_raw_symbols.append(raw_symbol)

    for index in range(0, len(new_raw_symbols), STREAMS_PER_CONNECTION):
        chunk = new_raw_symbols[index:index + STREAMS_PER_CONNECTION]
        connection_no = int(time.time() * 1000) % 1_000_000
        threading.Thread(
            target=websocket_worker,
            args=(chunk, connection_no),
            daemon=True,
        ).start()
    if new_raw_symbols:
        logger.info("Spot WebSocket aboneliği başlatıldı: %s coin", len(new_raw_symbols))


def initialize_csv():
    if TRADE_LOG_FILE.exists() and TRADE_LOG_FILE.stat().st_size:
        with TRADE_LOG_FILE.open("r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            existing_fields = next(reader, [])
        if existing_fields != CSV_FIELDS:
            try:
                datetime.fromisoformat(existing_fields[0])
                int(existing_fields[1])
            except (IndexError, ValueError):
                raise RuntimeError(
                    f"{TRADE_LOG_FILE} başlıkları beklenen şemayla uyuşmuyor; "
                    "mevcut kayıtlar değiştirilmedi."
                ) from None

            if (
                len(existing_fields) != len(CSV_FIELDS)
                or existing_fields[3] != "LONG"
                or existing_fields[4] not in {"FIRST", "SECOND", "LEGACY", "TOP30"}
            ):
                raise RuntimeError(
                    f"{TRADE_LOG_FILE} başlıkları beklenen şemayla uyuşmuyor; "
                    "mevcut kayıtlar değiştirilmedi."
                )

            temporary_file = TRADE_LOG_FILE.with_suffix(TRADE_LOG_FILE.suffix + ".tmp")
            try:
                with TRADE_LOG_FILE.open("r", newline="", encoding="utf-8") as source:
                    rows = csv.reader(source)
                    with temporary_file.open("w", newline="", encoding="utf-8") as target:
                        writer = csv.writer(target)
                        writer.writerow(CSV_FIELDS)
                        for row in rows:
                            if len(row) != len(CSV_FIELDS):
                                raise RuntimeError(
                                    f"{TRADE_LOG_FILE} içinde şemaya uymayan kayıt var; "
                                    "mevcut kayıtlar değiştirilmedi."
                                )
                            writer.writerow(row)
                temporary_file.replace(TRADE_LOG_FILE)
            finally:
                temporary_file.unlink(missing_ok=True)
        return

    with TRADE_LOG_FILE.open("w", newline="", encoding="utf-8") as file:
        csv.DictWriter(file, fieldnames=CSV_FIELDS).writeheader()


def save_state():
    state = {
        "version": 4,
        "main_balance": main_balance,
        "positions": positions,
        "stats": stats,
        "recent_closed": recent_closed[:50],
        "trade_id_counter": trade_id_counter,
    }
    temporary_file = STATE_FILE.with_suffix(".tmp")
    temporary_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary_file.replace(STATE_FILE)


def load_state():
    global main_balance, positions, stats, recent_closed
    global trade_id_counter

    if not STATE_FILE.exists():
        return

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    main_balance = float(state["main_balance"])
    positions = state["positions"]
    for position in positions.values():
        position.setdefault("current_price", position["entry_price"])
        position.setdefault("opened_at", time.time())
        position.setdefault("risk_usd", STOP_LOSS_USD)
        position.setdefault("trade_type", "TOP30")
        position.setdefault("score", 0.0)
        position.setdefault("momentum_60s", 0.0)
        position.setdefault("volume_60s", 0.0)
        position.setdefault("trades_60s", 0)
        position.setdefault("pressure", 0.5)
        if state.get("version", 1) < 4:
            position["tp_price"] = calculate_exit_price(position, TAKE_PROFIT_USD)
            position["stop_price"] = calculate_exit_price(position, -STOP_LOSS_USD)
            position["risk_usd"] = STOP_LOSS_USD
            position["trade_type"] = "TOP30"
    stats.update(state.get("stats", {}))
    recent_closed = state.get("recent_closed", [])
    trade_id_counter = int(state.get("trade_id_counter", 0))


def update_universe():
    global universe, volume_by_symbol, change_by_symbol, raw_to_unified, unified_to_raw
    global last_universe_update

    markets = exchange.load_markets()
    tickers = exchange.fetch_tickers()
    ranked = []
    market_symbols = {}
    for symbol, market in markets.items():
        if not market.get("active", True) or market.get("spot") is not True:
            continue
        if market.get("quote") != "USDT" or market.get("base") in EXCLUDED_BASES:
            continue
        ticker = tickers.get(symbol)
        if not ticker:
            continue
        quote_volume = float(ticker.get("quoteVolume") or 0.0)
        change_24h = float(ticker.get("percentage") or 0.0)
        if (
            quote_volume >= MIN_VOLUME_24H
            and float(ticker.get("last") or 0.0) > 0
            and change_24h > 0
        ):
            ranked.append((symbol, quote_volume, change_24h))
            raw_symbol = str(market.get("id") or symbol.replace("/", "")).upper()
            market_symbols[raw_symbol] = symbol

    ranked.sort(key=lambda row: (row[2], row[1]), reverse=True)
    selected = ranked[:MAX_UNIVERSE]
    universe = [symbol for symbol, _, _ in selected]
    volume_by_symbol = {symbol: volume for symbol, volume, _ in selected}
    change_by_symbol = {symbol: change for symbol, _, change in selected}
    selected_symbols = set(universe)
    raw_to_unified = {
        raw: symbol for raw, symbol in market_symbols.items()
        if symbol in selected_symbols
    }
    unified_to_raw = {
        symbol: raw for raw, symbol in raw_to_unified.items()
    }
    last_universe_update = time.monotonic()
    logger.info(
        "Spot 24s kazanan Top %s: %s parite, min hacim $%s",
        MAX_UNIVERSE,
        len(universe),
        f"{MIN_VOLUME_24H:,.0f}",
    )
    start_websockets_for(universe)


def equity():
    with lock:
        total = main_balance
        for position in positions.values():
            total += position["position_value"] + unrealized_pnl(position) + position["entry_fee"]
        return total


def unrealized_pnl(position):
    current = float(position.get("current_price") or 0.0)
    entry = float(position.get("entry_price") or 0.0)
    quantity = float(position.get("quantity") or 0.0)
    if current <= 0 or entry <= 0 or quantity <= 0:
        return 0.0
    executed_exit = current * (1.0 - SLIPPAGE_RATE)
    gross = (executed_exit - entry) * quantity
    exit_fee = executed_exit * quantity * FEE_RATE
    return gross - position["entry_fee"] - exit_fee


def open_position(candidate):
    global main_balance, trade_id_counter

    symbol = candidate["symbol"]
    if candidate.get("side") != "LONG":
        return False

    with lock:
        if symbol in positions or len(positions) >= MAX_OPEN_POSITIONS:
            return False

        entry = float(candidate["price"]) * (1.0 + SLIPPAGE_RATE)
        quantity = calculate_spot_quantity(symbol, entry, main_balance)
        if quantity <= 0:
            return False
        notional = quantity * entry
        entry_fee = notional * FEE_RATE
        position_template = {
            "entry_price": entry,
            "quantity": quantity,
            "entry_fee": entry_fee,
        }
        target = calculate_exit_price(position_template, TAKE_PROFIT_USD)
        stop = calculate_exit_price(position_template, -STOP_LOSS_USD)

        if quantity <= 0 or main_balance < notional + entry_fee:
            return False

        market = exchange.market(symbol)
        min_cost = float((market.get("limits", {}).get("cost") or {}).get("min") or 0.0)
        if min_cost and notional < min_cost:
            return False

        trade_id_counter += 1
        main_balance -= notional + entry_fee
        positions[symbol] = {
            "id": trade_id_counter,
            "symbol": symbol,
            "side": "LONG",
            "trade_type": "TOP30",
            "entry_price": entry,
            "current_price": entry,
            "tp_price": target,
            "stop_price": stop,
            "position_value": notional,
            "notional": notional,
            "quantity": quantity,
            "entry_fee": entry_fee,
            "opened_at": time.time(),
            "risk_usd": STOP_LOSS_USD,
            "score": candidate["score"],
            "momentum_60s": candidate["momentum"],
            "volume_60s": candidate["volume_60s"],
            "trades_60s": candidate["trades"],
            "pressure": candidate["buy_pressure"],
        }
        stats["opened"] += 1
        stats["total_fees"] += entry_fee
        save_state()
        logger.info(
            "SPOT PAPER ALIŞ %s miktar=%.8f maliyet=$%.2f giriş=%.8f "
            "net hedef=+$%.2f net stop=-$%.2f süre=1 saat skor=%.1f",
            symbol, quantity, notional, entry, TAKE_PROFIT_USD, STOP_LOSS_USD,
            candidate["score"],
        )
        return True


def close_position(symbol, exit_price, reason):
    global main_balance

    with lock:
        position = positions.get(symbol)
        if not position or exit_price <= 0:
            return False

        executed_exit = exit_price * (1.0 - SLIPPAGE_RATE)
        quantity = position["quantity"]
        gross = (executed_exit - position["entry_price"]) * quantity
        exit_fee = executed_exit * quantity * FEE_RATE
        net = gross - position["entry_fee"] - exit_fee
        main_balance += position["position_value"] + gross - exit_fee

        record = {
            "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "trade_id": position["id"],
            "symbol": symbol,
            "side": "LONG",
            "trade_type": position["trade_type"],
            "entry_price": position["entry_price"],
            "exit_price": executed_exit,
            "stop_price": position["stop_price"],
            "tp_price": position["tp_price"],
            "quantity": quantity,
            "position_value": position["position_value"],
            "gross_pnl": gross,
            "entry_fee": position["entry_fee"],
            "exit_fee": exit_fee,
            "net_pnl": net,
            "close_reason": reason,
            "hold_seconds": max(0.0, time.time() - position["opened_at"]),
            "risk_usd": position["risk_usd"],
            "score": position["score"],
            "momentum_60s": position["momentum_60s"],
            "volume_60s": position["volume_60s"],
            "trades_60s": position["trades_60s"],
            "pressure": position["pressure"],
        }
        with TRADE_LOG_FILE.open("a", newline="", encoding="utf-8") as file:
            csv.DictWriter(file, fieldnames=CSV_FIELDS).writerow(record)

        recent_closed.insert(0, record)
        del recent_closed[50:]
        del positions[symbol]
        stats["closed"] += 1
        stats["realized_pnl"] += net
        stats["total_fees"] += exit_fee
        stats["wins" if net > 0 else "losses"] += 1
        save_state()
        logger.info("PAPER SATIŞ %s çıkış=%.8f net P/L=$%.2f neden=%s", symbol, executed_exit, net, reason)
        return True


def check_position_tick(position, price):
    if price <= 0:
        return
    position["current_price"] = price
    if price <= position["stop_price"]:
        close_position(position["symbol"], price, "SL")
    elif price >= position["tp_price"]:
        close_position(position["symbol"], position["tp_price"], "TP")
    elif time.time() - position["opened_at"] >= MAX_HOLDING_SECONDS:
        close_position(position["symbol"], price, "TIME")


def monitor_positions():
    with lock:
        symbols = list(positions)
    for symbol in symbols:
        try:
            ticker = exchange.fetch_ticker(symbol)
            price = float(ticker.get("last") or 0.0)
            with lock:
                position = positions.get(symbol)
                if position and price > 0:
                    check_position_tick(position, price)
        except (ccxt.NetworkError, ccxt.ExchangeError, ValueError, TypeError) as error:
            logger.warning("%s fiyatı alınamadı; pozisyon izlenemedi: %s", symbol, error)


def analysis_cycle():
    global last_candidates, last_analysis_time

    analyses = []
    entry_candidates = []
    for symbol in universe:
        signal = get_60s_stats(symbol)
        row = signal or {
            "symbol": symbol,
            "side": "WAIT",
            "price": 0.0,
            "momentum": 0.0,
            "volume_60s": 0.0,
            "trades": 0,
            "buy_pressure": 0.5,
            "score": 0.0,
            "aligned": False,
            "volume_24h": volume_by_symbol.get(symbol, 0.0),
        }
        row["change_24h"] = change_by_symbol.get(symbol, 0.0)
        analyses.append(row)
        if not signal:
            continue
        if (
            signal["side"] == "LONG"
            and signal["volume_24h"] >= MIN_VOLUME_24H
            and abs(signal["momentum"]) >= MIN_MOMENTUM_PCT
            and signal["trades"] >= MIN_TRADES_60S
            and signal["aligned"]
            and signal["score"] >= MIN_SCORE
        ):
            entry_candidates.append(row)

    analyses.sort(key=lambda candidate: candidate["change_24h"], reverse=True)
    entry_candidates.sort(key=lambda candidate: candidate["score"], reverse=True)
    with lock:
        last_candidates = analyses[:MAX_UNIVERSE]
        last_analysis_time = time.time()
        slots = max(0, MAX_OPEN_POSITIONS - len(positions))

    opened = 0
    for candidate in entry_candidates:
        if opened >= slots:
            break
        if open_position(candidate):
            opened += 1
    save_state()
    logger.info(
        "60s spot analiz tamamlandı: %s parite verisi, %s long sinyali",
        len(analyses),
        len(entry_candidates),
    )


def dashboard_data():
    with lock:
        position_rows = []
        for position in positions.values():
            row = dict(position)
            row["unrealized"] = unrealized_pnl(position)
            row["hold_seconds"] = max(0, int(time.time() - position["opened_at"]))
            position_rows.append(row)

        available = main_balance
        reserved = sum(position["position_value"] for position in positions.values())
        open_pnl = sum(row["unrealized"] for row in position_rows)
        account_equity = available + reserved + open_pnl + sum(
            position["entry_fee"] for position in positions.values()
        )
        return {
            "time": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            "balance": available,
            "reserved": reserved,
            "open_pnl": open_pnl,
            "equity": account_equity,
            "pnl": account_equity - START_BALANCE,
            "pnl_pct": (account_equity / START_BALANCE - 1.0) * 100.0,
            "positions": position_rows,
            "candidates": list(last_candidates),
            "closed": list(recent_closed),
            "stats": dict(stats),
            "universe": len(universe),
            "last_analysis": (
                datetime.fromtimestamp(last_analysis_time, timezone.utc).strftime("%H:%M:%S UTC")
                if last_analysis_time else "-"
            ),
            "can_open_position": (
                main_balance >= POSITION_VALUE * (1.0 + FEE_RATE)
                and len(positions) < MAX_OPEN_POSITIONS
            ),
            "target_balance": START_BALANCE,
            "position_value": POSITION_VALUE,
            "take_profit_usd": TAKE_PROFIT_USD,
            "stop_loss_usd": STOP_LOSS_USD,
            "max_holding_seconds": MAX_HOLDING_SECONDS,
        }


HTML = r'''<!doctype html>
<html lang="tr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Spot Paper Bot</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#0b1020;color:#e8edf7;font-family:Inter,Segoe UI,Arial,sans-serif}
.container{max-width:1500px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;align-items:center;gap:15px;margin-bottom:18px}
h1{font-size:25px;margin:0}.sub{color:#91a0b8;font-size:13px;margin-top:5px}.live{padding:8px 12px;border-radius:20px;background:#102a1d;color:#61e294;font-size:12px;font-weight:700}
.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}.card{background:#11182b;border:1px solid #202a42;border-radius:14px;padding:15px}.label{color:#8190aa;font-size:12px}.value{font-size:22px;font-weight:750;margin-top:7px}.pos{color:#58dc91}.neg{color:#ff7180}.neutral{color:#e8edf7}
.grid{display:grid;grid-template-columns:1.15fr .85fr;gap:16px}.panel{background:#11182b;border:1px solid #202a42;border-radius:14px;padding:15px;margin-bottom:16px;overflow:hidden}.panel h2{font-size:15px;margin:0 0 12px}.scroll{overflow:auto}table{width:100%;border-collapse:collapse;min-width:700px}th,td{padding:10px 8px;border-bottom:1px solid #20243a;text-align:left;font-size:12px;white-space:nowrap}th{color:#7787a3;font-weight:600}td{color:#dce4f3}.long{color:#57dc91;font-weight:700}.score{font-weight:800}.small{font-size:11px;color:#8390a8}.empty{padding:28px;text-align:center;color:#71809a}.footer{color:#66748d;text-align:center;font-size:11px;padding:12px}
@media(max-width:1000px){.cards{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}}@media(max-width:600px){.container{padding:12px}h1{font-size:20px}.value{font-size:18px}}
</style></head><body><div class="container">
<div class="top"><div><h1>BINANCE SPOT PAPER BOT</h1><div class="sub">24s değişime göre en çok kazanan 30 spot coin • işlem başı $20 • 1 saatlik işlem sınırı</div></div><div class="live">● PAPER MODE</div></div>
<div class="cards">
<div class="card"><div class="label">SERBEST BAKİYE</div><div id="balance" class="value">$0.00</div></div>
<div class="card"><div class="label">AÇIK SPOT VARLIĞI</div><div id="reserved" class="value">$0.00</div></div>
<div class="card"><div class="label">AÇIK NET P/L</div><div id="openpnl" class="value">$0.00</div></div>
<div class="card"><div class="label">EQUITY</div><div id="equity" class="value">$0.00</div></div>
<div class="card"><div class="label">POZİSYON</div><div id="positions" class="value">0 / 10</div></div></div>
<div class="grid"><div>
<div class="panel"><h2>🏆 24 SAATLİK EN ÇOK KAZANAN 30 SPOT COIN</h2><div class="small">24s hacim en az $3M • Top 30, 24s yüzde değişimine göre sıralanır • giriş sinyali: 60s momentum/skor • yalnızca LONG</div><div class="scroll"><table><thead><tr><th>#</th><th>Coin</th><th>24s%</th><th>Sinyal</th><th>Skor</th><th>Momentum 60s</th><th>Alış baskısı</th><th>İşlem sayısı</th><th>Hacim 60s</th></tr></thead><tbody id="candidates"></tbody></table></div></div>
<div class="panel"><h2>📌 AÇIK SPOT POZİSYONLAR</h2><div class="scroll"><table><thead><tr><th>Coin</th><th>Giriş</th><th>Güncel</th><th>Hedef fiyat (+$2 net)</th><th>Stop fiyatı (-$1 net)</th><th>Net P/L</th><th>Süre</th></tr></thead><tbody id="openpos"></tbody></table></div></div>
</div><div>
<div class="panel"><h2>📊 İSTATİSTİKLER</h2><table><tbody><tr><td>Açılan</td><td id="opened">0</td></tr><tr><td>Kapanan</td><td id="closed">0</td></tr><tr><td>Kazanan</td><td id="wins">0</td></tr><tr><td>Kaybeden</td><td id="losses">0</td></tr><tr><td>Gerçekleşen P/L</td><td id="realized">$0.00</td></tr><tr><td>Toplam komisyon</td><td id="fees">$0.00</td></tr></tbody></table></div>
<div class="panel"><h2>✅ SON KAPANAN İŞLEMLER</h2><div class="scroll"><table><thead><tr><th>Coin</th><th>Giriş</th><th>Kapanış</th><th>Net P/L</th><th>Neden</th></tr></thead><tbody id="closedlist"></tbody></table></div></div>
<div class="panel"><h2>⚙️ SİSTEM</h2><table><tbody><tr><td>İzlenen Top 30</td><td id="universe">0</td></tr><tr><td>Son analiz</td><td id="analysis">-</td></tr><tr><td>Yeni işlem durumu</td><td id="limit">AKTİF</td></tr><tr><td>Bağlantı</td><td id="connection">Bağlı</td></tr></tbody></table></div>
</div></div>
<div class="footer">Paper simülasyonudur; canlı emir göndermez. Başlangıç bakiyesi $200. Her spot işlem $20; net TP +$2, net SL -$1. 1 saat sonunda hedef/stop yoksa piyasa fiyatından kapanır. Bakiye $200 altına inse de yeni $20 işlemi karşılanabildiği sürece devam eder. • <span id="clock">--:--:--</span></div></div>
<script>
function money(x){return '$'+Number(x).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}
function num(x,d=2){return Number(x).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})}
function cls(x){return x>0?'pos':x<0?'neg':'neutral'}
function render(d){
 document.getElementById('balance').textContent=money(d.balance);
 document.getElementById('reserved').textContent=money(d.reserved);
 let op=document.getElementById('openpnl');op.textContent=money(d.open_pnl);op.className='value '+cls(d.open_pnl);
 document.getElementById('equity').textContent=money(d.equity);
 document.getElementById('positions').textContent=d.positions.length+' / 10';
 document.getElementById('opened').textContent=d.stats.opened;
 document.getElementById('closed').textContent=d.stats.closed;
 document.getElementById('wins').textContent=d.stats.wins;
 document.getElementById('losses').textContent=d.stats.losses;
 let rp=document.getElementById('realized');rp.textContent=money(d.stats.realized_pnl);rp.className=cls(d.stats.realized_pnl);
 document.getElementById('fees').textContent=money(d.stats.total_fees);
 document.getElementById('universe').textContent=d.universe;
 document.getElementById('analysis').textContent=d.last_analysis;
 document.getElementById('limit').textContent=d.can_open_position?'AKTİF — $20 işlem bütçesi mevcut':'İŞLEM BÜTÇESİ YETERSİZ';
 let c=document.getElementById('candidates');c.innerHTML='';
 if(!d.candidates.length)c.innerHTML='<tr><td colspan="9" class="empty">Top 30 coin listesi bekleniyor.</td></tr>';
 d.candidates.forEach((x,i)=>{let tr=document.createElement('tr');let signalClass=x.side==='LONG'?'pos':'neg';tr.innerHTML=`<td>${i+1}</td><td><b>${x.symbol.split('/')[0]}</b></td><td class="${x.change_24h>=0?'pos':'neg'}">${x.change_24h>=0?'+':''}${num(x.change_24h,2)}%</td><td class="${signalClass}">${x.side}</td><td class="score">${num(x.score,1)}</td><td class="${x.momentum>=0?'pos':'neg'}">${x.momentum>=0?'+':''}${num(x.momentum,2)}%</td><td>${num(x.buy_pressure*100,1)}%</td><td>${num(x.trades,0)}</td><td>${money(x.volume_60s)}</td>`;c.appendChild(tr)});
 let p=document.getElementById('openpos');p.innerHTML='';
 if(!d.positions.length)p.innerHTML='<tr><td colspan="7" class="empty">Açık pozisyon yok.</td></tr>';
 d.positions.forEach(x=>{let tr=document.createElement('tr');tr.innerHTML=`<td><b>${x.symbol.split('/')[0]}</b></td><td>${num(x.entry_price,8)}</td><td>${num(x.current_price,8)}</td><td>${num(x.tp_price,8)}</td><td>${num(x.stop_price,8)}</td><td class="${cls(x.unrealized)}">${money(x.unrealized)}</td><td>${num(x.hold_seconds/60,1)} dk</td>`;p.appendChild(tr)});
 let cl=document.getElementById('closedlist');cl.innerHTML='';
 if(!d.closed.length)cl.innerHTML='<tr><td colspan="5" class="empty">Henüz kapanan işlem yok.</td></tr>';
 d.closed.forEach(x=>{let tr=document.createElement('tr');tr.innerHTML=`<td><b>${x.symbol.split('/')[0]}</b></td><td>${num(x.entry_price,8)}</td><td>${num(x.exit_price,8)}</td><td class="${cls(x.net_pnl)}">${money(x.net_pnl)}</td><td>${x.close_reason}</td>`;cl.appendChild(tr)});
 document.getElementById('clock').textContent=d.time;
}
async function refresh(){
 const status=document.getElementById('connection');
 try{const response=await fetch('/api');if(!response.ok)throw new Error('HTTP '+response.status);render(await response.json());status.textContent='Bağlı';status.className='pos'}
 catch(error){status.textContent='API hatası: '+error.message;status.className='neg'}
}
refresh();setInterval(refresh,1000);
</script></body></html>'''


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format_string, *args):
        return

    def do_GET(self):
        if self.path == "/api":
            body = json.dumps(dashboard_data(), ensure_ascii=False).encode("utf-8")
            content_type = "application/json; charset=utf-8"
            status = 200
        elif self.path == "/":
            body = HTML.encode("utf-8")
            content_type = "text/html; charset=utf-8"
            status = 200
        else:
            body = b"Not found"
            content_type = "text/plain; charset=utf-8"
            status = 404

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_dashboard():
    server = ThreadingHTTPServer((HTML_HOST, HTML_PORT), DashboardHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Dashboard: http://%s:%s", HTML_HOST, HTML_PORT)
    return server


def main():
    global running, last_universe_update

    load_state()
    initialize_csv()
    logger.info(
        "Spot paper bot başlıyor | bakiye=$%.2f / hedef=$%.2f | "
        "işlem başı=$%.2f | TP=+$%.2f net / SL=-$%.2f net | süre=%s dk",
        main_balance, START_BALANCE, POSITION_VALUE, TAKE_PROFIT_USD,
        STOP_LOSS_USD, MAX_HOLDING_SECONDS // 60,
    )
    logger.info("Canlı emir kapalı. Durum: %s", STATE_FILE.resolve())

    update_universe()
    server = start_dashboard()
    last_analysis = 0.0

    try:
        while running:
            now = time.monotonic()
            if now - last_universe_update >= UNIVERSE_REFRESH_SECONDS:
                try:
                    update_universe()
                except (ccxt.NetworkError, ccxt.ExchangeError) as error:
                    logger.exception("Parite evreni yenilenemedi: %s", error)

            monitor_positions()
            if now - last_analysis >= ANALYSIS_INTERVAL_SECONDS:
                try:
                    analysis_cycle()
                except Exception:
                    logger.exception("Analiz döngüsü beklenmeyen hatayla durdu")
                last_analysis = now
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Durdurma isteği alındı")
    finally:
        running = False
        save_state()
        server.shutdown()
        server.server_close()
        logger.info("Paper bot durduruldu; açık simülasyon pozisyonları kaydedildi")


if __name__ == "__main__":
    main()
