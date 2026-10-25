# ============================================================
# BINANCE FUTURES PAPER TRADER v6.3
# 60s TRADE ANALIZI + UZUN BEKLEYIS KURALI
# SADECE PAPER TRADING - GERCEK BINANCE EMRI GONDERMEZ
# ============================================================

import ccxt
import time
import csv
import os
import threading
from datetime import datetime, timezone

# ---------------------- AYARLAR ----------------------
START_BALANCE           = 1000.0
POSITION_MARGIN         = 20.0
LEVERAGE                = 5
POSITION_NOTIONAL       = POSITION_MARGIN * LEVERAGE
FEE_RATE                = 0.0005
SLIPPAGE_RATE           = 0.0008

MAX_POSITIONS           = 10
MAX_SAME_SIDE           = 5
MAX_DAILY_LOSS_PERCENT  = 0.05

FIRST_TP_PERCENT        = 0.10
FIRST_SL_PERCENT        = 0.10
SECOND_TP_PERCENT       = 0.05

ANALYSIS_WINDOW_SECONDS = 60
SCAN_INTERVAL           = 60
MIN_TRADES_60S          = 20
MIN_ABS_CHANGE_PERCENT  = 0.15
MIN_ACTIVITY_RATIO      = 1.0
REENTRY_COOLDOWN_SECONDS = 300

# -------- UZUN BEKLEYIS KURALI (YENI) --------
LONG_WAIT_SECONDS        = 600        # 10 dk = uzun bekleyis
LONG_WAIT_PROFIT_TARGET  = 5.0        # $5 kar -> kapat
LONG_WAIT_HARD_TIMEOUT   = 1800       # 30 dk sonunda ne olursa olsun kapat

PRICE_CHECK_INTERVAL    = 2
RENDER_INTERVAL         = 3
TOP_SYMBOLS_TO_SCAN     = 120
TOP_DISPLAY             = 10
TRADE_LOG               = "paper_trades.csv"

# ---------------------- DURUM ----------------------
free_balance        = START_BALANCE
positions           = {}      # symbol -> dict
cooldowns           = {}
daily_pnl           = 0.0
daily_reset_date    = datetime.now(timezone.utc).date()
daily_start_balance = START_BALANCE
stop_flag           = False
lock                = threading.Lock()

last_analysis   = []      # son 60s analiz sonuclari
last_closed     = []      # son kapanan islemler
last_prices     = {}      # symbol -> son fiyat
total_fees      = 0.0
wins            = 0
losses          = 0

exchange = ccxt.binanceusdm({
    "enableRateLimit": True,
    "options": {"defaultType": "future"},
})


# ---------------------- YARDIMCI ----------------------
def now_utc():
    return datetime.now(timezone.utc)


def log(msg):
    print(f"[{now_utc().strftime('%H:%M:%S')}] {msg}", flush=True)


def fmt_price(p):
    if p >= 100:    return f"{p:.2f}"
    if p >= 10:     return f"{p:.3f}"
    if p >= 1:      return f"{p:.4f}"
    if p >= 0.1:    return f"{p:.5f}"
    if p >= 0.01:   return f"{p:.6f}"
    return f"{p:.8f}"


def activity_label(ratio):
    if ratio < 0.5:  return "DUSUK"
    if ratio < 1.5:  return "ORTA"
    if ratio < 3.0:  return "ORTA+"
    return "YUKSEK"


def init_csv():
    if not os.path.exists(TRADE_LOG):
        with open(TRADE_LOG, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["zaman", "sembol", "yon", "giris", "cikis",
                        "miktar", "net_pnl", "sebep", "sure_sn"])


def log_trade_csv(symbol, side, entry, exit_p, qty, pnl, reason, duration):
    try:
        with open(TRADE_LOG, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                now_utc().isoformat(), symbol, side,
                round(entry, 10), round(exit_p, 10),
                round(qty, 8), round(pnl, 4),
                reason, round(duration, 1),
            ])
    except Exception as e:
        log(f"log_trade_csv hata: {e}")


def check_daily_reset():
    global daily_reset_date, daily_pnl, daily_start_balance
    today = now_utc().date()
    if today != daily_reset_date:
        daily_reset_date = today
        daily_pnl = 0.0
        daily_start_balance = free_balance + sum(p["margin"] for p in positions.values())


# ---------------------- POZISYON ISLEMLERI ----------------------
def open_position(symbol, side, price):
    global free_balance, total_fees
    with lock:
        if len(positions) >= MAX_POSITIONS:
            return False
        if symbol in positions:
            return False
        same_side = sum(1 for p in positions.values() if p["side"] == side)
        if same_side >= MAX_SAME_SIDE:
            return False
        if symbol in cooldowns and time.time() < cooldowns[symbol]:
            return False
        if free_balance < POSITION_MARGIN:
            return False

        if side == "long":
            entry = price * (1 + SLIPPAGE_RATE)
        else:
            entry = price * (1 - SLIPPAGE_RATE)

        qty = POSITION_NOTIONAL / entry
        entry_fee = POSITION_NOTIONAL * FEE_RATE

        free_balance -= POSITION_MARGIN
        free_balance -= entry_fee
        total_fees  += entry_fee

        positions[symbol] = {
            "side":         side,
            "entry":        entry,
            "qty":          qty,
            "original_qty": qty,
            "margin":       POSITION_MARGIN,
            "opened_at":    time.time(),
            "tp1_hit":      False,
            "realized":     -entry_fee,
        }
        return True


def close_position(symbol, price, reason, portion=1.0):
    global free_balance, daily_pnl, total_fees, wins, losses
    with lock:
        p = positions.get(symbol)
        if not p:
            return

        close_qty = p["qty"] * portion
        if close_qty <= 0:
            return

        if p["side"] == "long":
            exit_price = price * (1 - SLIPPAGE_RATE)
            pnl_gross = (exit_price - p["entry"]) * close_qty
        else:
            exit_price = price * (1 + SLIPPAGE_RATE)
            pnl_gross = (p["entry"] - exit_price) * close_qty

        exit_fee = exit_price * close_qty * FEE_RATE
        pnl_net = pnl_gross - exit_fee

        released_margin = p["margin"] * portion
        p["margin"]  -= released_margin
        p["qty"]     -= close_qty
        p["realized"] += pnl_net

        free_balance += released_margin + pnl_net
        daily_pnl    += pnl_net
        total_fees   += exit_fee

        is_full_close = p["qty"] < 1e-12
        if is_full_close:
            duration = time.time() - p["opened_at"]
            total_net = p["realized"]
            log_trade_csv(symbol, p["side"], p["entry"], exit_price,
                          p["original_qty"], total_net, reason, duration)
            last_closed.append({
                "time":   now_utc().strftime("%H:%M:%S"),
                "symbol": symbol.split("/")[0],
                "side":   p["side"].upper(),
                "pnl":    total_net,
                "reason": reason,
            })
            if len(last_closed) > 20:
                last_closed.pop(0)

            if total_net >= 0:
                wins += 1
            else:
                losses += 1

            del positions[symbol]
            cooldowns[symbol] = time.time() + REENTRY_COOLDOWN_SECONDS


# ---------------------- POZISYON TAKIP THREAD ----------------------
def check_positions():
    global stop_flag
    while not stop_flag:
        try:
            symbols = list(positions.keys())
            if not symbols:
                time.sleep(PRICE_CHECK_INTERVAL)
                continue

            for symbol in symbols:
                p = positions.get(symbol)
                if not p:
                    continue
                try:
                    ticker = exchange.fetch_ticker(symbol)
                    price = ticker["last"]
                    last_prices[symbol] = price
                except Exception:
                    continue

                duration = time.time() - p["opened_at"]

                if p["side"] == "long":
                    move = (price - p["entry"]) / p["entry"]
                    upnl = (price - p["entry"]) * p["qty"]
                else:
                    move = (p["entry"] - price) / p["entry"]
                    upnl = (p["entry"] - price) * p["qty"]

                # 1) STOP LOSS
                if move <= -FIRST_SL_PERCENT:
                    close_position(symbol, price, f"SL({move*100:.2f}%)")
                    continue

                # 2) TP1 -> %50 kapat
                if not p["tp1_hit"] and move >= FIRST_TP_PERCENT:
                    close_position(symbol, price, f"TP1({move*100:.2f}%)", portion=0.5)
                    if symbol in positions:
                        positions[symbol]["tp1_hit"] = True
                    continue

                # 3) TP2 -> kalan tamami
                if p["tp1_hit"] and move >= (FIRST_TP_PERCENT + SECOND_TP_PERCENT):
                    close_position(symbol, price, f"TP2({move*100:.2f}%)")
                    continue

                # 4) UZUN BEKLEYIS: 10dk sonra $5 kar -> kapat (YENI)
                if duration >= LONG_WAIT_SECONDS and upnl >= LONG_WAIT_PROFIT_TARGET:
                    close_position(symbol, price, f"UZUN_BEKLEYIS(+${upnl:.2f})")
                    continue

                # 5) HARD TIMEOUT: 30dk sonra ne olursa olsun kapat
                if duration >= LONG_WAIT_HARD_TIMEOUT:
                    close_position(symbol, price, f"TIMEOUT({duration:.0f}s)")
                    continue

        except Exception as e:
            log(f"check_positions hata: {e}")

        time.sleep(PRICE_CHECK_INTERVAL)


# ---------------------- PIYASA ANALIZI ----------------------
def fetch_raw_klines(symbol, interval="1m", limit=25):
    market = exchange.market(symbol)
    return exchange.fapiPublicGetKlines({
        "symbol":   market["id"],
        "interval": interval,
        "limit":    limit,
    })


def analyze_symbol(symbol):
    try:
        raw = fetch_raw_klines(symbol, "1m", 25)
        if len(raw) < 5:
            return None

        last = raw[-2]   # son kapanmis mum
        prev = raw[-3]

        open_p    = float(last[1])
        high_p    = float(last[2])
        low_p     = float(last[3])
        close_p   = float(last[4])
        volume    = float(last[5])
        quote_vol = float(last[7])
        trades    = int(last[8])
        taker_buy = float(last[9])
        prev_close = float(prev[4])

        change = (close_p - prev_close) / prev_close * 100

        vols = [float(c[5]) for c in raw[-22:-2]]
        avg_vol = sum(vols) / len(vols) if vols else 0
        activity_ratio = volume / avg_vol if avg_vol > 0 else 0

        pressure = (taker_buy / volume * 100) if volume > 0 else 50.0

        score = abs(change) * 40 + activity_ratio * 10 + abs(pressure - 50) * 0.5

        return {
            "symbol":         symbol.split("/")[0],
            "full_symbol":    symbol,
            "price":          close_p,
            "change":         change,
            "volume":         volume,
            "quote_volume":   quote_vol,
            "trades":         trades,
            "activity_ratio": activity_ratio,
            "activity":       activity_label(activity_ratio),
            "pressure":       pressure,
            "score":          score,
        }
    except Exception:
        return None


def scan_once():
    global last_analysis

    if daily_pnl <= -daily_start_balance * MAX_DAILY_LOSS_PERCENT:
        log("Gunluk zarar limiti asildi! Yeni pozisyon acilmiyor.")
        return

    try:
        tickers = exchange.fetch_tickers()
    except Exception as e:
        log(f"fetch_tickers hata: {e}")
        return

    candidates = []
    for sym, t in tickers.items():
        m = exchange.markets.get(sym)
        if not m:
            continue
        if not m.get("swap") or m.get("quote") != "USDT" or not m.get("linear"):
            continue
        if not m.get("active"):
            continue
        qv = t.get("quoteVolume") or 0
        candidates.append((qv, sym))

    candidates.sort(reverse=True)
    top_syms = [s for _, s in candidates[:TOP_SYMBOLS_TO_SCAN]]

    results = []
    for sym in top_syms:
        r = analyze_symbol(sym)
        if not r:
            continue
        if r["trades"] < MIN_TRADES_60S:
            continue
        results.append(r)

    results.sort(key=lambda x: -x["score"])
    last_analysis = results

    # Filtreli sinyaller
    longs  = [r for r in results
              if r["change"] >=  MIN_ABS_CHANGE_PERCENT
              and r["activity_ratio"] >= MIN_ACTIVITY_RATIO]
    shorts = [r for r in results
              if r["change"] <= -MIN_ABS_CHANGE_PERCENT
              and r["activity_ratio"] >= MIN_ACTIVITY_RATIO]

    longs.sort(key=lambda x: -x["change"])
    shorts.sort(key=lambda x:  x["change"])

    if len(positions) < MAX_POSITIONS:
        for s in longs:
            if len(positions) >= MAX_POSITIONS:
                break
            open_position(s["full_symbol"], "long", s["price"])

    if len(positions) < MAX_POSITIONS:
        for s in shorts:
            if len(positions) >= MAX_POSITIONS:
                break
            open_position(s["full_symbol"], "short", s["price"])


def scanner_loop():
    global stop_flag
    while not stop_flag:
        try:
            scan_once()
        except Exception as e:
            log(f"scanner hata: {e}")
        for _ in range(SCAN_INTERVAL):
            if stop_flag:
                break
            time.sleep(1)


# ---------------------- EKRAN ----------------------
def render_screen():
    os.system("cls" if os.name == "nt" else "clear")

    with lock:
        open_pnl = 0.0
        pos_rows = []
        for sym, p in positions.items():
            price = last_prices.get(sym, p["entry"])
            if p["side"] == "long":
                upnl = (price - p["entry"]) * p["qty"]
                tp = p["entry"] * (1 + FIRST_TP_PERCENT)
                sl = p["entry"] * (1 - FIRST_SL_PERCENT)
            else:
                upnl = (p["entry"] - price) * p["qty"]
                tp = p["entry"] * (1 - FIRST_TP_PERCENT)
                sl = p["entry"] * (1 + FIRST_SL_PERCENT)
            open_pnl += upnl
            pos_rows.append((sym, p, price, upnl, tp, sl))

        reserved = sum(p["margin"] for p in positions.values())
        equity = free_balance + reserved + open_pnl
        total_pnl = equity - START_BALANCE
        total_pnl_pct = total_pnl / START_BALANCE * 100
        total_trades = wins + losses
        wr = (wins / total_trades * 100) if total_trades > 0 else 0
        analysis_snapshot = list(last_analysis)
        closed_snapshot = list(last_closed)

    sep = "=" * 110
    thin = "-" * 110
    lines = []
    lines.append(sep)
    lines.append(f" BINANCE FUTURES PAPER TRADER v6.3 | 60s TRADE ANALIZI | {now_utc().strftime('%H:%M:%S')}")
    lines.append(sep)
    lines.append(f" Bakiye: {free_balance:>10.2f} USDT | Ayrilmis: {reserved:>6.2f} | "
                 f"Acik P/L: {open_pnl:>+7.2f} | Equity: {equity:>10.2f}")
    lines.append(f" Toplam P/L: {total_pnl:>+7.2f} USDT ({total_pnl_pct:>+.2f}%) | "
                 f"Pozisyon:{len(positions)}/{MAX_POSITIONS} | "
                 f"W:{wins} L:{losses} WR:%{wr:.0f} | Fee:{total_fees:.4f}")
    lines.append(thin)
    lines.append(" 60 SANIYELIK PIYASA ANALIZI")
    lines.append(thin)
    lines.append(" #  COIN         YON      60s%       HACIM60        ISLEM  AKTIVITE    BASKI    SKOR")
    lines.append(thin)

    if analysis_snapshot:
        for i, a in enumerate(analysis_snapshot[:TOP_DISPLAY], 1):
            side = "LONG" if a["change"] > 0 else "SHORT"
            lines.append(
                f"{i:>2} {a['symbol']:<12} {side:<8} "
                f"{a['change']:>+7.2f}%   {a['volume']:>10,.0f}  "
                f"{a['trades']:>5}  {a['activity']:<8}  "
                f"{a['pressure']:>5.1f}%  {a['score']:>6.1f}"
            )
    else:
        lines.append("  (analiz bekleniyor...)")

    lines.append(thin)
    lines.append(" ACIK POZISYONLAR")
    lines.append(thin)
    lines.append(" COIN         YON     GIRIS        GUNCEL       TP           STOP         P/L")
    lines.append(thin)

    if pos_rows:
        for sym, p, price, upnl, tp, sl in pos_rows:
            base = sym.split("/")[0]
            lines.append(
                f" {base:<12} {p['side'].upper():<6} "
                f"{fmt_price(p['entry']):>12} {fmt_price(price):>11} "
                f"{fmt_price(tp):>11} {fmt_price(sl):>11} "
                f"{upnl:>+7.2f} USDT"
            )
    else:
        lines.append("  (acik pozisyon yok)")

    lines.append(thin)
    lines.append(" SON KAPANAN ISLEMLER")
    lines.append(thin)

    if closed_snapshot:
        for c in closed_snapshot[-5:]:
            lines.append(f" {c['time']}  {c['symbol']:<12} {c['side']:<6} "
                         f"{c['pnl']:>+7.2f} USDT   {c['reason']}")
    else:
        lines.append(" Henuz kapanan islem yok.")

    lines.append(sep)
    print("\n".join(lines), flush=True)


def render_loop():
    global stop_flag
    while not stop_flag:
        try:
            render_screen()
        except Exception:
            pass
        time.sleep(RENDER_INTERVAL)


# ---------------------- ANA DONGU ----------------------
def main():
    global stop_flag

    init_csv()
    exchange.load_markets()

    t_scan = threading.Thread(target=scanner_loop,  daemon=True)
    t_pos  = threading.Thread(target=check_positions, daemon=True)
    t_ui   = threading.Thread(target=render_loop,   daemon=True)

    t_scan.start()
    t_pos.start()
    time.sleep(1)
    t_ui.start()

    try:
        while True:
            check_daily_reset()
            time.sleep(10)
    except KeyboardInterrupt:
        stop_flag = True
        time.sleep(0.5)
        print("\nPaper trader durduruldu. Gercek Binance emri gonderilmedi.")


if __name__ == "__main__":
    main()