import json
import websocket
from kafka import KafkaProducer

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: v.encode("utf-8")   # send raw CSV string
)

# Mapping symbols → IDs
symbol_id = {
    "BTCUSDT": 1,
    "ETHUSDT": 2,
    "BNBUSDT": 3,
    "SOLUSDT": 4
}

socket = "wss://stream.binance.com:9443/ws/btcusdt@ticker"

def on_message(ws, message):
    data = json.loads(message)

    ts = data["E"]
    symbol = data["s"]
    id_value = symbol_id.get(symbol, 999)      # fallback ID
    count = float(data["v"])                   # volume
    high = float(data["h"])
    low = float(data["l"])
    last_price = float(data["c"])              # close → used as vwap

    # Build CSV (9 columns)
    csv_line = f"{ts},{id_value},{count},0,{high},{low},0,0,{last_price}"

    print("Sending CSV to Kafka:", csv_line)

    producer.send("crypto", csv_line)

def on_open(ws):
    print("Connected to Binance WebSocket…")

websocket.WebSocketApp(
    socket,
    on_open=on_open,
    on_message=on_message
).run_forever()
