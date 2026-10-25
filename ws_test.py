import websocket
import json
import time

URL = "wss://fstream.binance.com/ws/btcusdt@kline_1m"

message_count = 0


def on_open(ws):

    print("=" * 60)
    print("BINANCE KLINE TEST")
    print("=" * 60)

    print("Bağlantı açıldı.")
    print("Stream:")
    print(URL)
    print()
    print("KLINE verisi bekleniyor...")
    print()


def on_message(ws, message):

    global message_count

    message_count += 1

    try:

        data = json.loads(message)

        if data.get("e") == "kline":

            k = data.get("k", {})

            print(
                f"[KLINE #{message_count}] "
                f"{data.get('s')} | "
                f"Open={k.get('o')} | "
                f"High={k.get('h')} | "
                f"Low={k.get('l')} | "
                f"Close={k.get('c')} | "
                f"Volume={k.get('q')} | "
                f"Closed={k.get('x')}"
            )

        else:

            print("MESAJ:", data)

    except Exception as e:

        print("PARSE HATASI:", repr(e))
        print(message)


def on_error(ws, error):

    print()
    print("=" * 60)
    print("WEBSOCKET HATASI")
    print("=" * 60)
    print(error)


def on_close(ws, code, msg):

    print()
    print("=" * 60)
    print("WEBSOCKET KAPANDI")
    print("=" * 60)
    print("Code:", code)
    print("Message:", msg)


ws = websocket.WebSocketApp(
    URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever(
    ping_interval=20,
    ping_timeout=10
)