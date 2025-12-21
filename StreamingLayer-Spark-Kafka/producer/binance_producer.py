# import json
# import threading
# import queue
# import websocket
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# import time
# import os

# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
# TOPIC = os.getenv("KAFKA_TOPIC")
# # KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# # TOPIC = "binance"

# # ============================================================
# # Queue buffer (giữa websocket và kafka)
# # ============================================================
# msg_queue = queue.Queue(maxsize=50000)

# # ============================================================
# # Hàm tạo producer (tự động retry)
# # ============================================================
# def create_producer():
#     while True:
#         try:
#             print(" Trying to connect Kafka...")
#             p = KafkaProducer(
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_serializer=lambda x: json.dumps(x).encode("utf-8"),
#                 linger_ms=5,
#                 batch_size=64 * 1024,
#                 max_request_size=2 * 1024 * 1024,
#                 retries=5
#             )
#             print(" Kafka connected!")
#             return p
#         except Exception as e:
#             print(" Kafka connect failed:", e)
#             time.sleep(3)

# producer = create_producer()

# # ============================================================
# # Worker thread gửi Kafka
# # ============================================================
# def kafka_worker():
#     global producer
#     counter = 0
#     while True:
#         msg = msg_queue.get()

#         try:
#             # ==========================================
#             #  LOG PAYLOAD TRƯỚC KHI GỬI KAFKA
#             # ==========================================
#             # small_json = json.dumps(msg)
#             # if len(small_json) < 5000:     # tránh spam log
#             #     print(f"\n PAYLOAD → {small_json}\n")

#             # future = producer.send(TOPIC, msg)
#             # result = future.get(timeout=5)
            

#             # print(
#             #     f" SENT → topic={result.topic}, "
#             #     f"partition={result.partition}, offset={result.offset}"
#             # )
#             producer.send(TOPIC, msg)
#             counter += 1
#             if counter % 100 == 0:
#                 print(f" Sent batch 100 messages. Current Queue size: {msg_queue.qsize()}")
#                 counter = 0

#         except KafkaError as e:
#             print(" Kafka send error:", e)
#             print(" Recreating Kafka producer...")
#             producer = create_producer()

#         except Exception as e:
#             print(" Unexpected Kafka error:", e)


# threading.Thread(target=kafka_worker, daemon=True).start()

# # ============================================================
# # WebSocket callbacks
# # ============================================================
# def on_message(ws, message):
#     try:
#         msg = json.loads(message)
#     except Exception:
#         print(" BAD JSON")
#         return

#     try:
#         msg_queue.put_nowait(msg)
#     except queue.Full:
#         print(" Queue FULL → dropping message")

# def on_error(ws, error):
#     print(" WebSocket Error:", error)

# def on_close(ws):
#     print(" Closed connection")

# def on_open(ws):
#     print("### Connected to Binance Multi-stream ###")

# # ============================================================
# # Run Binance WebSocket
# # ============================================================
# stream_url = (
#     "wss://stream.binance.com:9443/stream?streams="
#     "btcusdt@trade/ethusdt@trade/bnbusdt@trade"
# )

# while True:
#     try:
#         ws = websocket.WebSocketApp(
#             stream_url,
#             on_open=on_open,
#             on_message=on_message,
#             on_error=on_error,
#             on_close=on_close
#         )
#         ws.run_forever(ping_interval=20, ping_timeout=10)

#     except Exception as e:
#         print(" Websocket crashed:", e)

#     print(" Reconnecting WebSocket in 3 seconds...")
#     time.sleep(3)


import json
import threading
import queue
import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import os
import sys

# Lấy biến môi trường
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "binance")

print(f"Config: Server={KAFKA_BOOTSTRAP_SERVERS}, Topic={TOPIC}")

# ============================================================
# Queue buffer (Tăng maxsize để chịu tải lúc Kafka lag)
# ============================================================
msg_queue = queue.Queue(maxsize=50000)

# ============================================================
# Hàm tạo producer (QUAN TRỌNG: Thêm api_version)
# ============================================================
def create_producer():
    while True:
        try:
            print(" Trying to connect Kafka...")
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                # Tối ưu hiệu năng gửi batch
                linger_ms=20,           # Đợi 20ms để gom tin
                batch_size=32 * 1024,   # 32KB per batch
                compression_type='gzip', # Nén dữ liệu để gửi nhanh hơn
                retries=5,
                # --- FIX LỖI METADATA TẠI ĐÂY ---
                # Ép dùng protocol mới, tránh lỗi với Kafka 7.7.0
                api_version=(2, 6, 0) 
            )
            print(" Kafka connected successfully!")
            return p
        except Exception as e:
            print(f" Kafka connect failed: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

producer = create_producer()

# ============================================================
# Worker thread gửi Kafka (Mode: ASYNC - Tốc độ cao)
# ============================================================
def kafka_worker():
    global producer
    counter = 0
    
    print(" Kafka Worker started...")
    
    while True:
        # Lấy tin nhắn từ hàng đợi
        msg = msg_queue.get()

        try:
            # Gửi bất đồng bộ (Fire & Forget)
            # Không dùng future.get() để tránh chặn luồng
            producer.send(TOPIC, msg)
            
            # Logic Log thông minh: Chỉ in log mỗi 500 tin
            counter += 1
            if counter % 500 == 0:
                q_size = msg_queue.qsize()
                print(f" [Stats] Sent {counter} msgs. Queue size: {q_size}")
                
                # Cảnh báo nếu queue sắp tràn
                if q_size > 40000:
                    print("  WARNING: Queue is filling up! Producer is slower than WebSocket.")
                
                # Reset counter để tránh số quá lớn (tùy chọn)
                if counter > 100000: 
                    counter = 0

        except KafkaError as e:
            print(f" Kafka send error: {e}")
            # Nếu lỗi kết nối nặng, thử tạo lại producer
            time.sleep(1)
            # producer = create_producer() # (Optional: uncomment nếu muốn auto-reconnect logic ở đây)

        except Exception as e:
            print(f" Unexpected Error: {e}")

# Chạy worker ở background
threading.Thread(target=kafka_worker, daemon=True).start()

# ============================================================
# WebSocket callbacks
# ============================================================
def on_message(ws, message):
    try:
        # Parse nhanh
        msg = json.loads(message)
        # Đưa vào hàng đợi, không chờ (Non-blocking)
        msg_queue.put_nowait(msg)
    except queue.Full:
        # Nếu queue đầy, chấp nhận mất dữ liệu để không crash app
        print("  Queue FULL -> Dropping message (System overloaded)")
    except Exception:
        pass # Bỏ qua lỗi parse json hỏng

def on_error(ws, error):
    print(f" WebSocket Error: {error}")

# Cập nhật signature mới cho websocket-client >= 1.0
def on_close(ws, close_status_code, close_msg):
    print(f" Closed connection. Code: {close_status_code}, Msg: {close_msg}")

def on_open(ws):
    print("###  Connected to Binance WebSocket ###")

# ============================================================
# Main Loop - Reconnect WebSocket nếu bị ngắt
# ============================================================
stream_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade"

if __name__ == "__main__":
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # Ping mỗi 20s để giữ kết nối
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f" Main Loop Crashed: {e}")

        print(" Reconnecting WebSocket in 5 seconds...")
        time.sleep(5)