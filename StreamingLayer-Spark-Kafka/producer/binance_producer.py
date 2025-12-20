import json
import threading
import queue
import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "binance"

# ============================================================
# Queue buffer (giữa websocket và kafka)
# ============================================================
msg_queue = queue.Queue(maxsize=20000)

# ============================================================
# Hàm tạo producer (tự động retry)
# ============================================================
def create_producer():
    while True:
        try:
            print(" Trying to connect Kafka...")
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                linger_ms=5,
                batch_size=64 * 1024,
                max_request_size=2 * 1024 * 1024,
                retries=5
            )
            print(" Kafka connected!")
            return p
        except Exception as e:
            print(" Kafka connect failed:", e)
            time.sleep(3)

producer = create_producer()

# ============================================================
# Worker thread gửi Kafka
# ============================================================
def kafka_worker():
    global producer
    while True:
        msg = msg_queue.get()

        try:
            # ==========================================
            #  LOG PAYLOAD TRƯỚC KHI GỬI KAFKA
            # ==========================================
            small_json = json.dumps(msg)
            if len(small_json) < 5000:     # tránh spam log
                print(f"\n PAYLOAD → {small_json}\n")

            future = producer.send(TOPIC, msg)
            result = future.get(timeout=5)

            print(
                f" SENT → topic={result.topic}, "
                f"partition={result.partition}, offset={result.offset}"
            )

        except KafkaError as e:
            print(" Kafka send error:", e)
            print(" Recreating Kafka producer...")
            producer = create_producer()

        except Exception as e:
            print(" Unexpected Kafka error:", e)


threading.Thread(target=kafka_worker, daemon=True).start()

# ============================================================
# WebSocket callbacks
# ============================================================
def on_message(ws, message):
    try:
        msg = json.loads(message)
    except Exception:
        print(" BAD JSON")
        return

    try:
        msg_queue.put_nowait(msg)
    except queue.Full:
        print(" Queue FULL → dropping message")

def on_error(ws, error):
    print(" WebSocket Error:", error)

def on_close(ws):
    print(" Closed connection")

def on_open(ws):
    print("### Connected to Binance Multi-stream ###")

# ============================================================
# Run Binance WebSocket
# ============================================================
stream_url = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@trade/ethusdt@trade/bnbusdt@trade"
)

while True:
    try:
        ws = websocket.WebSocketApp(
            stream_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)

    except Exception as e:
        print(" Websocket crashed:", e)

    print(" Reconnecting WebSocket in 3 seconds...")
    time.sleep(3)

