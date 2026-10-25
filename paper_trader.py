# ============================================================
# BINANCE TOP GAINERS - HOURLY SIGNAL PAPER BOT (v2)
# 100 USDT bakiye, BTC hariç, işlem başına 10 USDT marjin, 10x
#
# v2 değişiklikleri:
#  - Çıkış matematiği düzeltildi: trailing artık kilitlenen kârı zararla
#    uyumlu tutar (eski: kazanç ~ +$0.9, kayıp ~ -$2.1)
#  - Gerçekçilik: slippage, funding, gözlenen fiyattan SL dolumu,
#    daha yüksek hacim filtresi
#  - Sinyal modları: MOMENTUM (eski) / FADE / PULLBACK
#  - Risk sınırları: max pozisyon, aynı yönde max, cooldown, günlük zarar limiti
#  - "Realize PnL" artık açılış komisyonunu ve funding'i de içerir
#  - Kapanan her işlem trades.csv dosyasına yazılır (MFE/MAE dahil)
#  - İşlem akışını dolduran WAIT logları kaldırıldı, kline sorguları azaltıldı
# ============================================================

import os, csv, time, json, threading, requests, random
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------- AYARLAR ----------------------
PAPER_TRADING       = True
START_BALANCE_USD   = 100.0
TRADE_MARGIN_USD    = 10.0
LEVERAGE            = 10
FEE_RATE            = 0.0004      # %0.04 (taker)
MAINTENANCE_RATE    = 0.005       # %0.5 bakım marjı (likidite için)
MANUAL_RESERVE_USD  = TRADE_MARGIN_USD * (1 + LEVERAGE * FEE_RATE)

# --- Gerçekçilik ---
MIN_QUOTE_VOLUME    = 1_000_000   # 24s USDT hacim alt sınırı (düşürdükçe daha fazla oynak coin girer)
SLIPPAGE_RATE       = 0.0007      # hacim bilgisi yoksa kullanılan varsayılan (manuel/yedek)
# Hacim düştükçe spread/kayma artar: (24s USDT hacim alt sınırı, giriş/çıkış başına kayma)
SLIPPAGE_TIERS = [
    (20_000_000, 0.0005),   # çok likit
    (5_000_000,  0.0007),
    (1_000_000,  0.0015),
    (0,          0.0030),   # ince tahta (MIN_QUOTE_VOLUME'u 1M altına çekersen)
]
SIMULATE_FUNDING    = True

# --- Çıkış (USD, 10 USDT marjin x 10 = 100 USDT pozisyonda $1 = %1 fiyat) ---
# Kademeli kâr al: (marjin üzerinden ROI %, pozisyonun kapanacak payı). Paylar toplamı 1.0 olmalı.
# 10x kaldıraçta ROI %100 = fiyatta %10, ROI %200 = fiyatta %20 hareket demektir.
# Tek hedef için örn.: [(100.0, 1.0)]  veya  [(200.0, 1.0)]
TAKE_PROFIT_LEVELS  = [(100.0, 0.5), (200.0, 0.5)]
TAKE_PROFIT_PCT     = TAKE_PROFIT_LEVELS[0][0]        # ilk hedef (trailing eşiği olarak da kullanılır)
TAKE_PROFIT_USD     = TRADE_MARGIN_USD * TAKE_PROFIT_PCT / 100.0

# Zarar durdur KAPALI: pozisyon likite olana kadar (≈ -%9.5 fiyat, marjinin tamamı) tutulur.
# Eski davranışa dönmek için USE_STOP_LOSS = True yap.
USE_STOP_LOSS       = False
STOP_LOSS_USD       = 2.0         # sadece USE_STOP_LOSS = True iken geçerli

# Trailing KAPALI: TP seviyesine gelince pozisyon sabit kâr ile kapanır.
# True yaparsan TP seviyesinde iz süren stop devreye girer (TRAILING_STOP_USD mesafeyle).
USE_TRAILING        = False
TRAILING_STOP_USD   = 2.0

# --- Risk sınırları (sadece otomatik işlemler) ---
MAX_OPEN_POSITIONS  = 5
MAX_SAME_SIDE       = 3           # aynı yönde (korelasyon) üst sınır
COOLDOWN_MINUTES    = 120         # SL/LİKİT sonrası aynı coine tekrar girme
MAX_DAILY_LOSS_USD  = 10.0        # UTC günü içinde bu zarara ulaşınca yeni işlem yok

# --- Sinyal ---
# "MOMENTUM": eski mantık (24s ve 1s aynı yönde -> o yöne gir; fiilen sadece LONG)
# "FADE"    : aşırı yükselmiş coinde pompa mumundan sonra ilk belirgin kırmızı mum -> SHORT
# "PULLBACK": güçlü 24s trendde 1 kırmızı mumluk geri çekilme -> LONG
SIGNAL_MODE             = "FADE"
FADE_MIN_24H_PCT        = 10.0
FADE_MIN_1H_MOVE_PCT    = 0.3
PULLBACK_MIN_24H_PCT    = 8.0
PULLBACK_MIN_DROP_PCT   = 0.5
PULLBACK_MAX_DROP_PCT   = 3.0

CHECK_INTERVAL      = 1.0
GAINER_REFRESH_INTERVAL = 15.0
SIGNAL_SCAN_BATCH_SIZE = 10
HTML_HOST, HTML_PORT = "127.0.0.1", 8080
TRADES_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.csv")
CANDLE_MS = 3_600_000

if SIGNAL_MODE not in ("MOMENTUM", "FADE", "PULLBACK"):
    raise SystemExit(f"Geçersiz SIGNAL_MODE: {SIGNAL_MODE}")
if (abs(sum(frac for _, frac in TAKE_PROFIT_LEVELS) - 1.0) > 1e-9
        or [pct for pct, _ in TAKE_PROFIT_LEVELS] != sorted(pct for pct, _ in TAKE_PROFIT_LEVELS)):
    raise SystemExit("TAKE_PROFIT_LEVELS: paylar toplamı 1.0 olmalı ve yüzdeler artan sırada olmalı")

def _tp_levels():
    """Trailing açıksa sadece ilk hedef (trailing eşiği) kullanılır."""
    if USE_TRAILING:
        return [(TAKE_PROFIT_USD / TRADE_MARGIN_USD * 100.0, 1.0)]
    return TAKE_PROFIT_LEVELS

# ---------------------- DURUM ----------------------
lock = threading.RLock()

grid_state = {
    "active_symbol": "",
    "top_gainers": [],
    "current_price": 0.0,
    "price_change_pct": 0.0,
    "last_update": "",
    "available_balance": START_BALANCE_USD,   # serbest USDT
    "used_margin": 0.0,                       # pozisyonlarda kilitli marjin
    "unrealized_pnl": 0.0,
    "total_realized_pnl": 0.0,                # komisyon + funding dahil
    "total_fees_paid": 0.0,
    "total_funding": 0.0,                     # + ise ödenen, - ise alınan
    "completed_trades": 0,
    "wins": 0,
    "losses": 0,
    "liquidations": 0,
    "positions": [],
    "signals": {},
    "history": [],
    "cooldowns": {},
    "day_key": "",
    "day_realized": 0.0,
    "paper_mode": PAPER_TRADING,
    "signal_mode": SIGNAL_MODE,
    "leverage": LEVERAGE,
    "trade_margin_usd": TRADE_MARGIN_USD,
    "manual_reserve_usd": MANUAL_RESERVE_USD,
    "take_profit_usd": TAKE_PROFIT_USD,
    "take_profit_pct": TAKE_PROFIT_PCT,
    "take_profit_levels": [list(level) for level in (TAKE_PROFIT_LEVELS if not USE_TRAILING else [(TAKE_PROFIT_PCT, 1.0)])],
    "stop_loss_usd": STOP_LOSS_USD if USE_STOP_LOSS else None,
    "trailing_stop_usd": TRAILING_STOP_USD if USE_TRAILING else None
}

def now_time():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _uid(prefix):
    return f"{prefix}_{int(time.time()*1000)}_{random.randint(100,999)}"

def _sign(side):
    return 1 if side == "LONG" else -1

def log_event(event_type, msg, price=0.0, pnl=0.0):
    with lock:
        grid_state["history"].insert(0, {
            "time": now_time(),
            "type": event_type,
            "msg": msg,
            "price": price,
            "pnl": pnl
        })
        if len(grid_state["history"]) > 200:
            grid_state["history"].pop()

def _roll_day():
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if grid_state["day_key"] != day:
        grid_state["day_key"] = day
        grid_state["day_realized"] = 0.0

def _add_day_pnl(amount):
    _roll_day()
    grid_state["day_realized"] += amount

def _calc_unrealized():
    return sum(
        _sign(pos["side"]) * pos["size"] * (pos["current_price"] - pos["entry_price"])
        for pos in grid_state["positions"]
    )

# ---------------------- İŞLEM KAYDI (CSV) ----------------------
CSV_FIELDS = [
    "closed_at_utc", "opened_at_utc", "symbol", "side", "source", "signal_mode",
    "reason", "entry", "exit", "size", "gross_pnl", "open_fee", "close_fee",
    "funding", "net_pnl", "hold_min", "change_24h_at_entry", "change_1h_at_entry",
    "max_fav_usd", "max_adv_usd", "volume_24h", "slippage_rate",
    "closed_fraction", "trade_net_total"
]

def append_trade_csv(row):
    try:
        if os.path.exists(TRADES_CSV):
            with open(TRADES_CSV, encoding="utf-8") as f:
                header = f.readline().strip()
            if header != ",".join(CSV_FIELDS):      # eski sürüm başlığı: karışmasın diye yedekle
                os.replace(TRADES_CSV, TRADES_CSV.replace(".csv", f"_old_{int(time.time())}.csv"))
        is_new = not os.path.exists(TRADES_CSV)
        with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if is_new:
                writer.writeheader()
            writer.writerow(row)
    except OSError as exc:
        print(f"trades.csv yazılamadı: {exc}")

def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds")

# ---------------------- REST API ----------------------
def fetch_top_30_margin_gainers():
    try:
        response = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=5)
        response.raise_for_status()
        res = response.json()
        if not isinstance(res, list):
            raise ValueError("Binance 24 saatlik ticker yanıtı liste değil")

        gainers = []
        for t in res:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT") or sym == "BTCUSDT":
                continue
            pct   = float(t.get("priceChangePercent", 0.0))
            vol   = float(t.get("quoteVolume", 0.0))
            price = float(t.get("lastPrice", 0.0))
            if vol >= MIN_QUOTE_VOLUME and price > 0:
                gainers.append({
                    "symbol": sym[:-4] + "/USDT",
                    "change_24h": pct,
                    "volume": vol,
                    "price": price
                })
        gainers.sort(key=lambda x: x["change_24h"], reverse=True)
        top_30 = gainers[:30]
        with lock:
            for gainer in top_30:
                signal = grid_state["signals"].get(gainer["symbol"], {})
                gainer["signal"] = signal.get("side", "WAIT")
                gainer["change_1h"] = signal.get("change_1h", 0.0)
                gainer["candle_time"] = signal.get("candle_time", "")
            grid_state["top_gainers"] = top_30
            if top_30:
                grid_state["active_symbol"] = top_30[0]["symbol"]
                grid_state["current_price"] = top_30[0]["price"]
                grid_state["price_change_pct"] = top_30[0]["change_24h"]
                grid_state["last_update"] = now_time()
        return top_30
    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
        log_event("ERROR", f"Top 30 verisi alınamadı: {exc}")
        return []

def get_mark_data(symbol):
    """Mark fiyat + funding bilgisi (premiumIndex)."""
    raw = symbol.replace("/", "")
    response = requests.get(
        "https://fapi.binance.com/fapi/v1/premiumIndex",
        params={"symbol": raw},
        timeout=3
    )
    response.raise_for_status()
    result = response.json()
    price = float(result.get("markPrice", 0.0))
    if price <= 0:
        raise ValueError(f"{symbol} için geçersiz mark fiyatı: {price}")
    return {
        "price": price,
        "funding_rate": float(result.get("lastFundingRate") or 0.0),
        "next_funding_time": int(result.get("nextFundingTime") or 0)
    }

def get_single_price(symbol):
    return get_mark_data(symbol)["price"]

def fetch_closed_hour_candles(symbol, count=3):
    """Tamamlanmış son `count` adet 1 saatlik mumu (eskiden yeniye) döndürür."""
    raw = symbol.replace("/", "")
    response = requests.get(
        "https://fapi.binance.com/fapi/v1/klines",
        params={"symbol": raw, "interval": "1h", "limit": count + 1},
        timeout=5
    )
    response.raise_for_status()
    candles = response.json()
    if not isinstance(candles, list):
        raise ValueError(f"{symbol} saatlik mum yanıtı liste değil")

    now_ms = int(time.time() * 1000)
    closed = [candle for candle in candles if int(candle[6]) < now_ms]
    if not closed:
        raise ValueError(f"{symbol} için tamamlanmış 1 saatlik mum bulunamadı")

    return [
        {"candle_time": int(c[0]), "open": float(c[1]), "close": float(c[4])}
        for c in closed[-count:]
    ]

# ---------------------- DOLUM SİMÜLASYONU ----------------------
def slippage_for_volume(volume):
    if not volume:
        return SLIPPAGE_RATE
    for min_volume, rate in SLIPPAGE_TIERS:
        if volume >= min_volume:
            return rate
    return SLIPPAGE_TIERS[-1][1]

def _fill_price(price, side, action, rate=None):
    """Slippage: alım her zaman biraz pahalı, satım her zaman biraz ucuz dolar."""
    rate = SLIPPAGE_RATE if rate is None else rate
    is_buy = (side == "LONG" and action == "open") or (side == "SHORT" and action == "close")
    return price * (1 + rate) if is_buy else price * (1 - rate)

def _apply_funding(pos, mark):
    """Funding zamanı geçtiyse (nextFundingTime ilerlediyse) funding işler.
    Funding > 0 iken LONG öder, SHORT alır."""
    if not SIMULATE_FUNDING:
        return
    next_time = mark.get("next_funding_time", 0)
    if not next_time:
        return
    stored = pos.get("next_funding_time", 0)
    if not stored:
        pos["next_funding_time"] = next_time
    elif next_time > stored:
        cost = pos["size"] * pos["current_price"] * mark["funding_rate"] * _sign(pos["side"])
        grid_state["available_balance"] -= cost
        grid_state["total_funding"] += cost
        grid_state["total_realized_pnl"] -= cost
        _add_day_pnl(-cost)
        pos["funding_paid"] += cost
        pos["next_funding_time"] = next_time
        log_event("FUNDING", f"{pos['symbol']} {pos['side']} funding {'ödendi' if cost > 0 else 'alındı'}: ${abs(cost):.4f}")

# ---------------------- POZİSYON AÇMA ----------------------
def _open_position(symbol, side, mark, meta=None):
    meta = meta or {}
    d = _sign(side)
    lev = LEVERAGE
    margin = TRADE_MARGIN_USD
    slip = slippage_for_volume(meta.get("volume", 0.0))
    entry = _fill_price(mark["price"], side, "open", slip)
    notional = margin * lev
    size = notional / entry
    open_fee = notional * FEE_RATE
    if grid_state["available_balance"] < margin + open_fee:
        log_event("ERROR", f"{symbol}: bakiye yetersiz, {side} açılamadı (gerekli ${margin + open_fee:.2f})")
        return False
    grid_state["available_balance"] -= (margin + open_fee)
    grid_state["used_margin"]       += margin
    grid_state["total_fees_paid"]   += open_fee
    pos = {
        "id": _uid("L" if side == "LONG" else "S"),
        "symbol": symbol,
        "side": side,
        "source": meta.get("source", "AUTO"),
        "signal_mode": meta.get("signal_mode", "MANUAL"),
        "change_24h": meta.get("change_24h", 0.0),
        "change_1h": meta.get("change_1h", 0.0),
        "volume_24h": meta.get("volume", 0.0),
        "slippage_rate": slip,
        "entry_price": entry,
        "size": size,
        "margin": margin,
        "notional": notional,
        "leverage": lev,
        "open_fee": open_fee,
        "funding_paid": 0.0,
        "next_funding_time": mark.get("next_funding_time", 0),
        "liq_price": entry * (1 - d * (1 / lev - MAINTENANCE_RATE)),
        "size_orig": size,
        "realized_net": 0.0,                                # önceki kısmi kapanışların net toplamı
        "tp_levels": [
            {"price": entry + d * (margin * pct / 100.0) / size, "frac": frac}
            for pct, frac in _tp_levels()
        ],
        "tp_stage": 0,
        "tp_total": len(_tp_levels()),
        "tp_price": entry + d * (margin * _tp_levels()[0][0] / 100.0) / size,  # sıradaki kâr al (veya trailing eşiği)
        "sl_price": (entry - d * STOP_LOSS_USD / size) if USE_STOP_LOSS
                    else entry * (1 - d * (1 / lev - MAINTENANCE_RATE)),  # SL yok: likidite fiyatı
        "trailing_active": False,
        "best_price": entry,                                # lehteki en uç fiyat
        "max_fav_usd": 0.0,
        "max_adv_usd": 0.0,
        "current_price": mark["price"],
        "opened_at": time.time()
    }
    grid_state["positions"].append(pos)
    icon = "🟢" if side == "LONG" else "🔴"
    log_event("OPEN", f"{icon} {symbol} {side} açıldı {size:.6f} @ ${entry:.6f} | marjin ${margin:.2f} | {lev}x | {pos['signal_mode']}", entry)
    return True

# ---------------------- POZİSYON KAPATMA ----------------------
def _close_position(pos, exit_price, reason, size_part=None):
    """reason: 'TP', 'TP1', 'TP2', ..., 'SL', 'TRAIL', 'LIQ' veya 'MANUAL'.
    size_part verilirse pozisyonun sadece o kısmı kapanır (kademeli kâr al)."""
    side = pos["side"]
    d = _sign(side)
    partial = size_part is not None and reason != "LIQ" and size_part < pos["size"] - 1e-12
    part_size = size_part if partial else pos["size"]
    ratio = part_size / pos["size"]
    part_margin = pos["margin"] * ratio
    part_open_fee = pos["open_fee"] * ratio
    part_funding = pos["funding_paid"] * ratio

    if reason == "LIQ":
        # Likidite: kalan marjinin tamamı gider
        fill = exit_price
        close_fee = 0.0
        gross_pnl = -part_margin
        balance_return = 0.0
    else:
        fill = _fill_price(exit_price, side, "close", pos.get("slippage_rate"))
        gross_pnl = d * part_size * (fill - pos["entry_price"])
        close_fee = part_size * fill * FEE_RATE
        balance_return = max(0.0, part_margin + gross_pnl - close_fee)

    pnl_after_close = balance_return - part_margin
    realized = pnl_after_close - part_open_fee            # funding ayrıca işlendi
    leg_net = realized - part_funding
    total_net = pos["realized_net"] + leg_net             # işlemin tüm bacaklarının toplamı

    grid_state["available_balance"]  += balance_return
    grid_state["used_margin"]        -= part_margin
    grid_state["total_realized_pnl"] += realized
    grid_state["total_fees_paid"]    += close_fee
    _add_day_pnl(realized)

    if partial:
        log_event("CLOSE", f"✅ {pos['symbol']} {side} {reason}: pozisyonun %{part_size / pos['size_orig'] * 100:.0f}'i kapandı ${leg_net:+.2f} @ ${fill:.4f}", fill, leg_net)
    elif reason == "LIQ":
        grid_state["liquidations"] += 1
        log_event("LIQ", f"💀 {pos['symbol']} {side} LİKİT: ${total_net:+.2f} @ ${fill:.4f}", fill, total_net)
    else:
        grid_state["completed_trades"] += 1
        if total_net > 0:
            grid_state["wins"] += 1
        else:
            grid_state["losses"] += 1
        if reason.startswith("TP"):
            label, icon = "kâr al", "✅"
        elif reason == "TRAIL":
            label, icon = "iz süren kâr koruma", "✅"
        elif reason == "SL":
            label, icon = "zarar durdur", "⛔"
        else:
            label, icon = "manuel", "🔄"
        log_event("CLOSE", f"{icon} {pos['symbol']} {side} {label} ({reason}): işlem toplamı ${total_net:+.2f} @ ${fill:.4f}", fill, total_net)

    if reason in ("SL", "LIQ"):
        grid_state["cooldowns"][pos["symbol"]] = time.time() + COOLDOWN_MINUTES * 60

    append_trade_csv({
        "closed_at_utc": _iso(time.time()),
        "opened_at_utc": _iso(pos["opened_at"]),
        "symbol": pos["symbol"],
        "side": side,
        "source": pos["source"],
        "signal_mode": pos["signal_mode"],
        "reason": reason,
        "entry": f"{pos['entry_price']:.8f}",
        "exit": f"{fill:.8f}",
        "size": f"{part_size:.8f}",
        "gross_pnl": f"{gross_pnl:.4f}",
        "open_fee": f"{part_open_fee:.4f}",
        "close_fee": f"{close_fee:.4f}",
        "funding": f"{part_funding:.4f}",
        "net_pnl": f"{leg_net:.4f}",
        "hold_min": f"{(time.time() - pos['opened_at']) / 60:.1f}",
        "change_24h_at_entry": f"{pos['change_24h']:.2f}",
        "change_1h_at_entry": f"{pos['change_1h']:.2f}",
        "max_fav_usd": f"{pos['max_fav_usd']:.4f}",
        "max_adv_usd": f"{pos['max_adv_usd']:.4f}",
        "volume_24h": f"{pos['volume_24h']:.0f}",
        "slippage_rate": f"{pos['slippage_rate']:.4f}",
        "closed_fraction": f"{part_size / pos['size_orig']:.3f}",
        "trade_net_total": f"{total_net:.4f}",
    })

    if partial:
        pos["notional"] -= pos["notional"] * ratio
        pos["size"] -= part_size
        pos["margin"] -= part_margin
        pos["open_fee"] -= part_open_fee
        pos["funding_paid"] -= part_funding
        pos["realized_net"] = total_net
    else:
        grid_state["positions"].remove(pos)

def open_manual_position(symbol, side):
    if side not in ("LONG", "SHORT"):
        raise ValueError("Yön LONG veya SHORT olmalıdır")
    with lock:
        if not any(gainer["symbol"] == symbol for gainer in grid_state["top_gainers"]):
            raise ValueError("Coin güncel Top 30 listesinde değil")
        if any(pos["symbol"] == symbol for pos in grid_state["positions"]):
            raise ValueError(f"{symbol} için zaten açık pozisyon var")

    mark = get_mark_data(symbol)
    with lock:
        gainer = next((g for g in grid_state["top_gainers"] if g["symbol"] == symbol), None)
        if gainer is None:
            raise ValueError("Coin artık güncel Top 30 listesinde değil")
        if any(pos["symbol"] == symbol for pos in grid_state["positions"]):
            raise ValueError(f"{symbol} için zaten açık pozisyon var")
        meta = {
            "source": "MANUAL",
            "signal_mode": "MANUAL",
            "change_24h": gainer.get("change_24h", 0.0),
            "change_1h": gainer.get("change_1h", 0.0),
            "volume": gainer.get("volume", 0.0),
        }
        if not _open_position(symbol, side, mark, meta):
            raise ValueError("Bakiye yetersiz, pozisyon açılamadı")

def close_manual_position(position_id):
    with lock:
        position = next(
            (pos for pos in grid_state["positions"] if pos["id"] == position_id),
            None
        )
        if position is None:
            raise KeyError("Açık pozisyon bulunamadı")
        symbol = position["symbol"]

    exit_price = get_single_price(symbol)
    with lock:
        if position not in grid_state["positions"]:
            raise KeyError("Pozisyon kapatma isteğinden önce kapanmış")
        position["current_price"] = exit_price
        _close_position(position, exit_price, "MANUAL")
        grid_state["unrealized_pnl"] = _calc_unrealized()

def close_all_manual_positions():
    with lock:
        positions = list(grid_state["positions"])

    prices = {
        symbol: get_single_price(symbol)
        for symbol in {position["symbol"] for position in positions}
    }

    with lock:
        closed_count = 0
        for position in positions:
            if position not in grid_state["positions"]:
                continue
            exit_price = prices[position["symbol"]]
            position["current_price"] = exit_price
            _close_position(position, exit_price, "MANUAL")
            closed_count += 1
        grid_state["unrealized_pnl"] = _calc_unrealized()

    return closed_count

# ---------------------- SİNYAL ----------------------
def _candle_pct(candle):
    return (candle["close"] - candle["open"]) / candle["open"] * 100 if candle["open"] else 0.0

def decide_signal(change_24h, candles):
    """candles: tamamlanmış mumlar, eskiden yeniye. 'LONG' / 'SHORT' / 'WAIT' döner."""
    last = candles[-1]
    prev = candles[-2] if len(candles) >= 2 else None
    ch1 = _candle_pct(last)
    prev_ch = _candle_pct(prev) if prev else 0.0

    if SIGNAL_MODE == "MOMENTUM":
        if change_24h > 0 and ch1 > 0:
            return "LONG"
        if change_24h < 0 and ch1 < 0:
            return "SHORT"
    elif SIGNAL_MODE == "FADE":
        if change_24h >= FADE_MIN_24H_PCT and prev_ch > 0 and ch1 <= -FADE_MIN_1H_MOVE_PCT:
            return "SHORT"
        if change_24h <= -FADE_MIN_24H_PCT and prev_ch < 0 and ch1 >= FADE_MIN_1H_MOVE_PCT:
            return "LONG"
    elif SIGNAL_MODE == "PULLBACK":
        if (change_24h >= PULLBACK_MIN_24H_PCT and prev_ch > 0
                and -PULLBACK_MAX_DROP_PCT <= ch1 <= -PULLBACK_MIN_DROP_PCT):
            return "LONG"
    return "WAIT"

def _auto_open_block_reason(symbol, side):
    """Otomatik işlemin açılmasını engelleyen sebep (yoksa None). lock altında çağrılır."""
    _roll_day()
    if grid_state["cooldowns"].get(symbol, 0) > time.time():
        return "cooldown (yakın zamanda SL/likit)"
    if grid_state["day_realized"] <= -MAX_DAILY_LOSS_USD:
        return f"günlük zarar limiti (${MAX_DAILY_LOSS_USD:.0f}) doldu"
    if len(grid_state["positions"]) >= MAX_OPEN_POSITIONS:
        return f"en fazla {MAX_OPEN_POSITIONS} açık pozisyon"
    if sum(1 for p in grid_state["positions"] if p["side"] == side) >= MAX_SAME_SIDE:
        return f"aynı yönde en fazla {MAX_SAME_SIDE} pozisyon"
    required = TRADE_MARGIN_USD + TRADE_MARGIN_USD * LEVERAGE * FEE_RATE
    if grid_state["available_balance"] < required + MANUAL_RESERVE_USD:
        return f"manuel işlem için ${MANUAL_RESERVE_USD:.2f} bakiye korunuyor"
    return None

def process_hourly_signal(symbol):
    with lock:
        gainer = next(
            (item for item in grid_state["top_gainers"] if item["symbol"] == symbol),
            None
        )
        if gainer is None:
            return
        change_24h = float(gainer.get("change_24h", 0.0))
        volume = float(gainer.get("volume", 0.0))
        previous = dict(grid_state["signals"].get(symbol, {}))

    # Bir sonraki mum kapanmadan tekrar sorgulama (API tasarrufu)
    if previous.get("candle_time") and int(time.time() * 1000) < previous["candle_time"] + 2 * CANDLE_MS:
        return

    candles = fetch_closed_hour_candles(symbol, 3)
    candle = candles[-1]
    with lock:
        if grid_state["signals"].get(symbol, {}).get("candle_time") == candle["candle_time"]:
            return

    side = decide_signal(change_24h, candles)
    change_1h = _candle_pct(candle)
    signal = {
        "side": side,
        "change_1h": change_1h,
        "change_24h": change_24h,
        "candle_time": candle["candle_time"]
    }

    def record_signal():
        grid_state["signals"][symbol] = signal
        for g in grid_state["top_gainers"]:
            if g["symbol"] == symbol:
                g.update(signal)
                break

    with lock:
        if side == "WAIT":
            record_signal()          # WAIT artık işlem akışına log yazmaz
            return
        if any(pos["symbol"] == symbol for pos in grid_state["positions"]):
            record_signal()
            log_event("SIGNAL", f"{symbol}: {side} sinyali, mevcut pozisyon korundu")
            return
        blocked = _auto_open_block_reason(symbol, side)
        if blocked:
            record_signal()
            log_event("SIGNAL", f"{symbol}: {side} sinyali atlandı — {blocked}")
            return

    mark = get_mark_data(symbol)
    with lock:
        if grid_state["signals"].get(symbol, {}).get("candle_time") == candle["candle_time"]:
            return
        record_signal()
        if any(pos["symbol"] == symbol for pos in grid_state["positions"]):
            return
        blocked = _auto_open_block_reason(symbol, side)
        if blocked:
            log_event("SIGNAL", f"{symbol}: {side} sinyali atlandı — {blocked}")
            return
        _open_position(symbol, side, mark, {
            "source": "AUTO",
            "signal_mode": SIGNAL_MODE,
            "change_24h": change_24h,
            "change_1h": change_1h,
            "volume": volume,
        })

# ---------------------- POZİSYON TAKİBİ ----------------------
def update_open_positions():
    with lock:
        symbols = {pos["symbol"] for pos in grid_state["positions"]}

    marks = {}
    for symbol in symbols:
        try:
            marks[symbol] = get_mark_data(symbol)
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            log_event("ERROR", f"{symbol} pozisyon fiyatı alınamadı: {exc}")

    with lock:
        for pos in list(grid_state["positions"]):
            mark = marks.get(pos["symbol"])
            if mark is None:
                continue
            price = mark["price"]
            d = _sign(pos["side"])
            pos["current_price"] = price
            _apply_funding(pos, mark)

            pnl_now = d * pos["size"] * (price - pos["entry_price"])
            pos["max_fav_usd"] = max(pos["max_fav_usd"], pnl_now)
            pos["max_adv_usd"] = min(pos["max_adv_usd"], pnl_now)

            if d * (price - pos["liq_price"]) <= 0:
                _close_position(pos, pos["liq_price"], "LIQ")
                continue

            # SL kapalıysa sadece trailing devredeyken stop kontrolü yapılır
            if d * (price - pos["sl_price"]) <= 0 and (USE_STOP_LOSS or pos["trailing_active"]):
                # Fiyat 1 sn'lik örneklemede stop'u aşmış olabilir: gözlenen fiyattan dolum
                reason = "TRAIL" if pos["trailing_active"] else "SL"
                _close_position(pos, price, reason)
                continue

            pos["best_price"] = max(pos["best_price"], price) if d == 1 else min(pos["best_price"], price)
            if USE_TRAILING:
                if d * (price - pos["tp_price"]) >= 0:
                    pos["trailing_active"] = True
            else:
                # Kademeli kâr al: limit emir gibi hedef fiyatlarından dolar
                closed_fully = False
                while pos["tp_stage"] < pos["tp_total"]:
                    level = pos["tp_levels"][pos["tp_stage"]]
                    if d * (price - level["price"]) < 0:
                        break
                    last = pos["tp_stage"] == pos["tp_total"] - 1
                    label = f"TP{pos['tp_stage'] + 1}" if pos["tp_total"] > 1 else "TP"
                    if last:
                        _close_position(pos, level["price"], label)
                        closed_fully = True
                        break
                    _close_position(pos, level["price"], label, size_part=pos["size_orig"] * level["frac"])
                    pos["tp_stage"] += 1
                    pos["tp_price"] = pos["tp_levels"][pos["tp_stage"]]["price"]
                if closed_fully:
                    continue
            if pos["trailing_active"]:
                new_sl = pos["best_price"] - d * TRAILING_STOP_USD / pos["size"]
                pos["sl_price"] = max(pos["sl_price"], new_sl) if d == 1 else min(pos["sl_price"], new_sl)

        grid_state["unrealized_pnl"] = _calc_unrealized()

def scanner_and_grid_loop():
    last_gainer_refresh = 0.0
    last_summary = 0.0
    scan_index = 0
    while True:
        try:
            now = time.monotonic()
            if now - last_summary >= 300:
                last_summary = now
                with lock:
                    sigs = [g.get("signal", "WAIT") for g in grid_state["top_gainers"]]
                    top = grid_state["top_gainers"][:1]
                print(f"[{now_time()}] Top {len(sigs)} coin | LONG {sigs.count('LONG')} • SHORT {sigs.count('SHORT')} • WAIT {sigs.count('WAIT')}"
                      + (f" | lider: {top[0]['symbol']} %{top[0]['change_24h']:.1f}" if top else ""))
            if now - last_gainer_refresh >= GAINER_REFRESH_INTERVAL:
                fetch_top_30_margin_gainers()
                last_gainer_refresh = now

            with lock:
                gainers = list(grid_state["top_gainers"])
            if gainers:
                scan_index %= len(gainers)
                scan_batch = [
                    gainers[index % len(gainers)]
                    for index in range(scan_index, scan_index + SIGNAL_SCAN_BATCH_SIZE)
                ]
                scan_index = (scan_index + SIGNAL_SCAN_BATCH_SIZE) % len(gainers)
                for gainer in scan_batch:
                    try:
                        process_hourly_signal(gainer["symbol"])
                    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
                        symbol = gainer["symbol"]
                        log_event("ERROR", f"{symbol} saatlik sinyal alınamadı: {exc}")
            update_open_positions()
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            log_event("ERROR", f"Tarama döngüsü hatası: {exc}")
            print(f"Tarama döngüsü hatası: {exc}")
        time.sleep(CHECK_INTERVAL)

# ---------------------- HTML DASHBOARD ----------------------
HTML_PAGE = r'''<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Top 30 Hourly Signal Simulator</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#080a0f;color:#c9d1d9;font-family:Inter,Segoe UI,Arial,sans-serif;font-size:12px}
.container{max-width:1500px;margin:auto;padding:14px}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;background:#11151c;padding:14px;border-radius:8px;border:1px solid #1f242d}
h1{font-size:18px;margin:0;color:#f0b90b}.sub{color:#8b949e;font-size:11px;margin-top:3px}
.badge{padding:5px 10px;border-radius:12px;font-size:10px;font-weight:700;background:#f0b90b;color:#000}
.cards{display:grid;grid-template-columns:repeat(9,1fr);gap:8px;margin-bottom:12px}
.card{background:#11151c;border:1px solid #1f242d;border-radius:8px;padding:10px}
.label{color:#8b949e;font-size:10px;letter-spacing:.3px}
.value{font-size:15px;font-weight:700;margin-top:4px}
.pos{color:#2ea043}.neg{color:#f85149}
.grid-main{display:grid;grid-template-columns:1fr 1fr 320px;gap:12px}
.panel{background:#11151c;border:1px solid #1f242d;border-radius:8px;padding:12px}
.panel h2{font-size:12px;margin:0 0 10px;color:#f0f6fc;display:flex;justify-content:space-between;align-items:center}
.scroll{overflow:auto;max-height:520px}
table{width:100%;border-collapse:collapse}
th,td{padding:5px 7px;border-bottom:1px solid #1f242d;text-align:left;font-size:11px}
th{color:#8b949e;font-size:10px;text-transform:uppercase}
.long-row{background:rgba(63,185,80,0.05)}
.short-row{background:rgba(248,81,73,0.05)}
.buy-row{color:#3fb950}
.sell-row{color:#f85149}
.btn-select{background:#1f242d;color:#c9d1d9;border:1px solid #30363d;padding:2px 6px;border-radius:4px;cursor:pointer;font-size:10px}
.btn-select:hover{background:#f0b90b;color:#000}
.btn-close-all{background:#da3633;color:#fff;border:1px solid #f85149;padding:4px 8px;border-radius:4px;cursor:pointer;font-size:10px}
.btn-close-all:disabled{opacity:.45;cursor:not-allowed}
.history-type{padding:2px 5px;border-radius:3px;font-size:9px;font-weight:bold}
.type-OPEN{background:#1f6beb;color:#fff}
.type-CLOSE{background:#238636;color:#fff}
.type-LIQ{background:#da3633;color:#fff}
.type-SYSTEM{background:#6e40c9;color:#fff}
.tag{padding:1px 5px;border-radius:3px;font-size:9px;font-weight:bold}
.tag-long{background:#238636;color:#fff}
.tag-short{background:#da3633;color:#fff}
</style>
</head>
<body>
<div class="container">
  <div class="top">
    <div>
            <h1>TOP 30 KAZANAN — 1 SAATLİK SİNYAL SİMÜLASYONU</h1>
            <div class="sub">Binance USDT-M • BTC hariç • Gerçek emir gönderilmez • Slippage + funding simülasyonu • Kâr eşiğinden sonra iz süren stop • Manuel işlem için bakiye ayrılır</div>
    </div>
        <div><span id="leverage-badge" class="badge">10x • 10 USDT/işlem • +$2 / -$2</span></div>
  </div>

  <div class="cards">
    <div class="card"><div class="label">TOP 30 LİDER COİN</div><div id="active-coin" class="value" style="color:#f0b90b">-</div></div>
    <div class="card"><div class="label">FİYAT</div><div id="price" class="value">$0.00</div><div id="price-updated" class="sub">Güncelleme bekleniyor</div></div>
        <div class="card"><div class="label">LİDERİN 24S DEĞİŞİMİ</div><div id="price-change" class="value">0.0000%</div></div>
    <div class="card"><div class="label">SERBEST</div><div id="avail" class="value">$100.00</div></div>
    <div class="card"><div class="label">KİLİTLİ MARJİN</div><div id="usedm" class="value">$0.00</div></div>
    <div class="card"><div class="label">AÇIK PnL</div><div id="unpnl" class="value">$0.00</div></div>
    <div class="card"><div class="label">REALİZE PnL</div><div id="realpnl" class="value pos">$0.00</div></div>
    <div class="card"><div class="label">KOMİSYON</div><div id="fees" class="value neg">$0.00</div><div id="funding" class="sub">funding: $0.00</div></div>
    <div class="card"><div class="label">LİKİT / İŞLEM</div><div id="stats" class="value">0 / 0</div></div>
  </div>

  <div class="grid-main">
    <div class="panel">
      <h2>📌 AÇIK POZİSYONLAR <button id="close-all-button" class="btn-close-all" onclick="manualCloseAll()" disabled>Tümünü Kapat</button></h2>
      <div class="scroll">
        <table>
          <thead><tr><th>COIN</th><th>YÖN</th><th>GİRİŞ</th><th>FİYAT</th><th>TP</th><th>SL / LİKİT</th><th>MİKTAR</th><th>MARJİN</th><th>PnL</th><th>AÇIK SÜRE</th><th>İŞLEM</th></tr></thead>
          <tbody id="pos-tbody"></tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>🔥 TOP 30 KAZANANLAR</h2>
      <div class="scroll" style="max-height:560px">
        <table>
          <thead><tr><th>#</th><th>COIN</th><th>24s%</th><th>1s%</th><th>SİNYAL</th><th>MANUEL</th></tr></thead>
          <tbody id="top30-tbody"></tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>📜 İŞLEM AKIŞI</h2>
      <div class="scroll" style="max-height:560px">
        <table>
          <thead><tr><th>SAAT</th><th>TİP</th><th>DETAY</th></tr></thead>
          <tbody id="log-tbody"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
function fmt(n, d=2){ return Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d}); }

async function sendAction(action, payload){
  const response=await fetch('/api/'+action,{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify(payload)
  });
  const result=await response.json();
  if(!response.ok)throw new Error(result.error||'İşlem başarısız');
  await updateDashboard();
  return result;
}

async function manualOpen(symbol, side){
  try{await sendAction('open',{symbol,side});}
  catch(error){alert(error.message);}
}

async function manualClose(positionId){
  if(!confirm('Bu pozisyon güncel piyasa fiyatından kapatılsın mı?'))return;
  try{await sendAction('close',{position_id:positionId});}
  catch(error){alert(error.message);}
}

async function manualCloseAll(){
  if(!confirm('Tüm açık pozisyonlar güncel piyasa fiyatlarından kapatılsın mı?'))return;
  try{
    const result=await sendAction('close-all',{});
    alert(result.closed_count+' pozisyon kapatıldı.');
  }
  catch(error){alert(error.message);}
}

async function updateDashboard(){
  try {
    let r = await fetch('/api/data');
    let d = await r.json();

    document.getElementById('active-coin').textContent = d.active_symbol || '-';
    document.getElementById('leverage-badge').textContent = d.signal_mode + ' • ' + d.leverage + 'x • ' + d.trade_margin_usd.toFixed(0) + ' USDT/işlem • TP ' + d.take_profit_levels.map(l => '%' + l[0] + (d.take_profit_levels.length > 1 ? ' (' + Math.round(l[1]*100) + '%)' : '')).join(' → ') + (d.stop_loss_usd == null ? ' • SL yok (likite kadar)' : ' • SL -$' + d.stop_loss_usd) + (d.trailing_stop_usd == null ? '' : ' • trailing -$' + d.trailing_stop_usd);
    document.getElementById('price').textContent = '$' + fmt(d.current_price, 4);
    document.getElementById('price-updated').textContent = d.last_update ? 'Son güncelleme: ' + d.last_update + ' UTC' : 'Güncelleme bekleniyor';
    let pc = Number(d.price_change_pct || 0);
    let pcEl = document.getElementById('price-change');
    pcEl.textContent = (pc >= 0 ? '+' : '') + pc.toFixed(4) + '%';
    pcEl.className = 'value ' + (pc >= 0 ? 'pos' : 'neg');
    document.getElementById('avail').textContent = '$' + fmt(d.available_balance, 2);
    document.getElementById('usedm').textContent = '$' + fmt(d.used_margin, 2);

    let u = document.getElementById('unpnl');
    u.textContent = (d.unrealized_pnl >= 0 ? '+' : '') + '$' + fmt(d.unrealized_pnl, 2);
    u.className = 'value ' + (d.unrealized_pnl >= 0 ? 'pos' : 'neg');

    let rp = document.getElementById('realpnl');
    rp.textContent = (d.total_realized_pnl >= 0 ? '+' : '') + '$' + fmt(d.total_realized_pnl, 2);
    rp.className = 'value ' + (d.total_realized_pnl >= 0 ? 'pos' : 'neg');

    document.getElementById('fees').textContent = '-$' + fmt(d.total_fees_paid, 2);
    document.getElementById('funding').textContent = 'funding: ' + (d.total_funding >= 0 ? '-' : '+') + '$' + fmt(Math.abs(d.total_funding), 2);
    document.getElementById('stats').textContent = d.liquidations + ' / ' + d.completed_trades + ' (K' + d.wins + '-Z' + d.losses + ')';

    const nowSeconds=Date.now()/1000;
    const openSymbols=new Set(d.positions.map(p=>p.symbol));

    // Pozisyonlar
    let pt = document.getElementById('pos-tbody');
    pt.innerHTML = '';
    document.getElementById('close-all-button').disabled = d.positions.length === 0;
    d.positions.forEach(p => {
      let pnl;
      if(p.side === 'LONG') pnl = p.size * (p.current_price - p.entry_price);
      else pnl = p.size * (p.entry_price - p.current_price);
      let tr = document.createElement('tr');
      tr.className = p.side === 'LONG' ? 'long-row' : 'short-row';
      const elapsedMinutes=Math.max(0,(nowSeconds-p.opened_at)/60);
      tr.innerHTML = `<td>${p.symbol}</td>
                      <td><span class="tag tag-${p.side.toLowerCase()}">${p.side}</span></td>
                      <td>$${fmt(p.entry_price,4)}</td>
                      <td>$${fmt(p.current_price,4)}</td>
                      <td style="color:#2ea043">$${fmt(p.tp_price,4)}${p.trailing_active?' (trailing)':(p.tp_total>1?' (TP'+(p.tp_stage+1)+')':'')}</td>
                      <td style="color:#f85149">$${fmt(p.sl_price,4)}${p.trailing_active?' (takip)': ''}</td>
                      <td>${fmt(p.size,4)}</td>
                      <td>$${fmt(p.margin,2)}</td>
                      <td class="${pnl>=0?'pos':'neg'}">${pnl>=0?'+':''}$${fmt(pnl,2)}</td>
                      <td>${fmt(elapsedMinutes,1)} dk</td>`;
      const closeCell=document.createElement('td');
      const closeButton=document.createElement('button');
      closeButton.className='btn-select';
      closeButton.textContent='Kapat';
      closeButton.addEventListener('click',()=>manualClose(p.id));
      closeCell.appendChild(closeButton);
      tr.appendChild(closeCell);
      pt.appendChild(tr);
    });

    // Top 30
    let t30 = document.getElementById('top30-tbody');
    t30.innerHTML = '';
    d.top_gainers.forEach((g, i) => {
      let tr = document.createElement('tr');
      const signalClass = g.signal === 'LONG' ? 'pos' : (g.signal === 'SHORT' ? 'neg' : '');
      tr.innerHTML = `<td>${i+1}</td><td><b>${g.symbol.split('/')[0]}</b></td><td class="${g.change_24h>=0?'pos':'neg'}">${g.change_24h>=0?'+':''}${g.change_24h.toFixed(1)}%</td><td>${Number(g.change_1h || 0).toFixed(2)}%</td><td class="${signalClass}">${g.signal || 'WAIT'}</td>`;
      const actionCell=document.createElement('td');
      for(const side of ['LONG','SHORT']){
        const button=document.createElement('button');
        button.className='btn-select';
        button.textContent=side;
        button.disabled=openSymbols.has(g.symbol);
        button.style.marginRight='4px';
        button.addEventListener('click',()=>manualOpen(g.symbol,side));
        actionCell.appendChild(button);
      }
      tr.appendChild(actionCell);
      t30.appendChild(tr);
    });

    // Log
    let lb = document.getElementById('log-tbody');
    lb.innerHTML = '';
    d.history.filter(h => ['OPEN','CLOSE','LIQ'].includes(h.type)).forEach(h => {
      let tr = document.createElement('tr');
      tr.innerHTML = `<td>${h.time}</td><td><span class="history-type type-${h.type}">${h.type}</span></td><td>${h.msg}</td>`;
      lb.appendChild(tr);
    });

  } catch(e){ console.error('Dashboard güncellenemedi:', e); }
}

setInterval(updateDashboard, 1000);
updateDashboard();
</script>
</body>
</html>
'''

# ---------------------- HTTP SERVER ----------------------
class WebServer(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def _send_json(self, status, payload):
        body=json.dumps(payload,ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/data":
            with lock:
                data = json.loads(json.dumps(grid_state, ensure_ascii=False))
            self._send_json(200,data)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode('utf-8'))

    def do_POST(self):
        if self.path not in ("/api/open","/api/close","/api/close-all"):
            self._send_json(404,{"error":"API endpoint bulunamadı"})
            return

        if self.path == "/api/close-all":
            try:
                closed_count = close_all_manual_positions()
            except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
                log_event("ERROR",f"Tüm pozisyonlar kapatılırken Binance fiyatı alınamadı: {exc}")
                self._send_json(502,{"error":"Binance fiyatı alınamadı; pozisyon durumu değiştirilmedi"})
            else:
                self._send_json(200,{"ok":True,"closed_count":closed_count})
            return

        try:
            content_length=int(self.headers.get("Content-Length","0"))
            if content_length <= 0 or content_length > 4096:
                self._send_json(400 if content_length <= 0 else 413,{"error":"Geçersiz istek boyutu"})
                return
            payload=json.loads(self.rfile.read(content_length))
            if not isinstance(payload,dict):
                self._send_json(400,{"error":"JSON nesnesi bekleniyor"})
                return
        except (ValueError,UnicodeDecodeError) as exc:
            self._send_json(400,{"error":f"Geçersiz JSON: {exc}"})
            return

        try:
            if self.path == "/api/open":
                symbol=payload.get("symbol")
                side=payload.get("side")
                if not isinstance(symbol,str) or side not in ("LONG","SHORT"):
                    self._send_json(400,{"error":"Coin ve LONG/SHORT yönü gerekli"})
                    return
                open_manual_position(symbol,side)
            else:
                position_id=payload.get("position_id")
                if not isinstance(position_id,str):
                    self._send_json(400,{"error":"Pozisyon kimliği gerekli"})
                    return
                close_manual_position(position_id)
        except ValueError as exc:
            self._send_json(409,{"error":str(exc)})
        except KeyError as exc:
            self._send_json(404,{"error":str(exc).strip("'")})
        except requests.RequestException as exc:
            log_event("ERROR",f"Manuel işlem için Binance fiyatı alınamadı: {exc}")
            self._send_json(502,{"error":"Binance fiyatı alınamadı; pozisyon durumu değiştirilmedi"})
        else:
            self._send_json(200,{"ok":True})

# ---------------------- BAŞLAT ----------------------
if __name__ == "__main__":
    print("=" * 60)
    print(f"{LEVERAGE}x TOP 30 SAATLİK SİNYAL PAPER BOT BAŞLATILIYOR... (mod: {SIGNAL_MODE})")
    print("TP " + (" → ".join(f"%{pct:.0f} ({frac*100:.0f}%)" for pct, frac in _tp_levels())) + " • " + (f"SL -${STOP_LOSS_USD}" if USE_STOP_LOSS else "SL YOK (likite kadar tutulur)") + (f" • trailing -${TRAILING_STOP_USD}" if USE_TRAILING else "") + f" • işlem kaydı: {TRADES_CSV}")
    print(f"{START_BALANCE_USD:.0f} USDT BAKİYE • İŞLEM BAŞINA {TRADE_MARGIN_USD:.0f} USDT MARJİN • BTC HARİÇ")
    print(f"Web Arayüz: http://{HTML_HOST}:{HTML_PORT}")
    print("=" * 60)

    threading.Thread(target=scanner_and_grid_loop, daemon=True).start()

    server = ThreadingHTTPServer((HTML_HOST, HTML_PORT), WebServer)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nBot durduruldu.")