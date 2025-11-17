# Technical Deep Dive: Production-Grade Implementation

## Enterprise Cryptocurrency Market Intelligence Platform

---

## I. DATA SOURCES & INGESTION

### 1.1 Multi-Source Data Collection Strategy

#### Real-time Data Sources (Streaming)

```python
DATA_SOURCES = {
"binance_websocket": {
"endpoint": "wss://stream.binance.com:9443/ws",
"streams": [
"!ticker@arr", # All tickers
"btcusdt@trade", # Individual trades
"btcusdt@depth20", # Order book
"btcusdt@kline_1m" # 1-minute candles
],
"throughput": "100K messages/sec",
"latency": "<50ms"
},
"coinbase_websocket": {
"endpoint": "wss://ws-feed.exchange.coinbase.com",
"channels": ["ticker", "matches", "level2"],
"throughput": "50K messages/sec"
},
"twitter_streaming": {
"api": "Twitter Streaming API v2",
"keywords": ["bitcoin", "crypto", "btc", "$BTC"],
"throughput": "10K tweets/hour",
"sentiment_analysis": "real-time"
},
"blockchain_mempool": {
"source": "Bitcoin/Ethereum nodes",
"data": "Pending transactions",
"insight": "Network congestion, fee estimation"
}
}
```

#### Batch Data Sources (Historical)

```python
BATCH_SOURCES = {
"coingecko_api": {
"endpoint": "https://api.coingecko.com/api/v3",
"data": "Historical prices, market cap, volume",
"frequency": "daily",
"retention": "5 years",
"volume": "2GB/day"
},
"glassnode_api": {
"data": "On-chain metrics (MVRV, NVT, etc.)",
"frequency": "daily",
"use_case": "Fundamental analysis"
},
"alternative_me": {
"data": "Fear & Greed Index",
"frequency": "daily",
"use_case": "Market sentiment"
}
}
```

### 1.2 Production Kafka Configuration

```yaml
# kafka-cluster-config.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
name: crypto-kafka-cluster
namespace: ingestion
spec:
kafka:
version: 3.6.0
replicas: 3
listeners:
- name: plain
port: 9092
type: internal
tls: false
- name: tls
port: 9093
type: internal
tls: true
config:
# Performance tuning
num.network.threads: 8
num.io.threads: 16
socket.send.buffer.bytes: 102400
socket.receive.buffer.bytes: 102400
socket.request.max.bytes: 104857600

# Replication & durability
default.replication.factor: 3
min.insync.replicas: 2
replica.fetch.max.bytes: 1048576

# Retention
log.retention.hours: 168 # 7 days
log.segment.bytes: 1073741824
log.retention.check.interval.ms: 300000

# Compression
compression.type: lz4

# Transaction support
transaction.state.log.replication.factor: 3
transaction.state.log.min.isr: 2

storage:
type: persistent-claim
size: 1000Gi
class: fast-ssd

resources:
requests:
memory: 32Gi
cpu: 8
limits:
memory: 64Gi
cpu: 16

zookeeper:
replicas: 3
storage:
type: persistent-claim
size: 100Gi
resources:
requests:
memory: 4Gi
cpu: 2
limits:
memory: 8Gi
cpu: 4
```

### 1.3 Kafka Topics Design

```python
KAFKA_TOPICS = {
# Raw data topics
"crypto.trades.raw": {
"partitions": 50,
"replication_factor": 3,
"retention_ms": 604800000, # 7 days
"compression": "lz4",
"description": "Raw trade execution data",
"schema": {
"exchange": "string",
"symbol": "string",
"price": "double",
"quantity": "double",
"timestamp": "long",
"trade_id": "string",
"is_buyer_maker": "boolean"
},
"throughput": "50K msg/sec"
},

"crypto.orderbook.raw": {
"partitions": 30,
"replication_factor": 3,
"retention_ms": 172800000, # 2 days
"compression": "lz4",
"description": "Order book snapshots",
"schema": {
"symbol": "string",
"bids": "array<{price: double, quantity: double}>",
"asks": "array<{price: double, quantity: double}>",
"timestamp": "long"
},
"throughput": "30K msg/sec"
},

"crypto.ohlcv.1m": {
"partitions": 20,
"replication_factor": 3,
"retention_ms": 2592000000, # 30 days
"compression": "snappy",
"description": "1-minute OHLCV candles",
"schema": {
"symbol": "string",
"open": "double",
"high": "double",
"low": "double",
"close": "double",
"volume": "double",
"timestamp": "long"
}
},

# Processed data topics
"crypto.features.realtime": {
"partitions": 20,
"replication_factor": 3,
"retention_ms": 86400000, # 1 day
"description": "Real-time engineered features",
"schema": {
"symbol": "string",
"features": "map<string, double>",
"timestamp": "long"
}
},

"crypto.predictions.realtime": {
"partitions": 10,
"replication_factor": 3,
"retention_ms": 86400000,
"description": "ML model predictions",
"schema": {
"symbol": "string",
"predicted_price": "double",
"confidence": "double",
"model_version": "string",
"timestamp": "long"
}
},

"crypto.alerts.critical": {
"partitions": 5,
"replication_factor": 3,
"retention_ms": 2592000000, # 30 days
"description": "Critical market alerts",
"schema": {
"alert_type": "string",
"symbol": "string",
"severity": "string",
"message": "string",
"metadata": "map<string, string>",
"timestamp": "long"
}
},

# Audit & compliance
"crypto.audit.transactions": {
"partitions": 10,
"replication_factor": 3,
"retention_ms": 15552000000, # 180 days
"cleanup_policy": "compact",
"description": "Audit trail for compliance"
}
}
```

### 1.4 Advanced Data Ingestion Pipeline

```python
"""
Production Kafka Producer with Retry, Circuit Breaker, Dead Letter Queue
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from tenacity import retry, stop_after_attempt, wait_exponential
from circuit_breaker import CircuitBreaker
import prometheus_client as prom

class ProductionKafkaProducer:
def __init__(self, bootstrap_servers, topic_config):
# Prometheus metrics
self.messages_sent = prom.Counter(
'kafka_messages_sent_total',
'Total messages sent',
['topic', 'status']
)
self.message_latency = prom.Histogram(
'kafka_message_latency_seconds',
'Message send latency',
['topic']
)

# Producer configuration
self.producer = KafkaProducer(
bootstrap_servers=bootstrap_servers,
value_serializer=lambda v: json.dumps(v).encode('utf-8'),
key_serializer=lambda k: k.encode('utf-8') if k else None,

# Performance
batch_size=32768, # 32KB batches
linger_ms=10, # Wait 10ms to batch
buffer_memory=67108864, # 64MB buffer
compression_type='lz4',

# Reliability
acks='all', # Wait for all replicas
retries=10,
max_in_flight_requests_per_connection=5,
enable_idempotence=True, # Exactly-once semantics

# Timeout
request_timeout_ms=30000,
metadata_max_age_ms=300000
)

self.dlq_producer = KafkaProducer(
bootstrap_servers=bootstrap_servers,
value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

self.circuit_breaker = CircuitBreaker(
failure_threshold=5,
recovery_timeout=60,
expected_exception=KafkaError
)

@retry(
stop=stop_after_attempt(3),
wait=wait_exponential(multiplier=1, min=2, max=10)
)
@CircuitBreaker.decorator
def send_message(self, topic, key, value, headers=None):
"""Send message with retry and circuit breaker"""
try:
start_time = time.time()

future = self.producer.send(
topic=topic,
key=key,
value=value,
headers=headers
)

# Wait for acknowledgment
record_metadata = future.get(timeout=10)

# Record metrics
latency = time.time() - start_time
self.message_latency.labels(topic=topic).observe(latency)
self.messages_sent.labels(topic=topic, status='success').inc()

return record_metadata

except KafkaError as e:
self.messages_sent.labels(topic=topic, status='error').inc()

# Send to Dead Letter Queue
self.send_to_dlq(topic, key, value, error=str(e))

raise

def send_to_dlq(self, original_topic, key, value, error):
"""Send failed message to Dead Letter Queue"""
dlq_topic = f"{original_topic}.dlq"
dlq_message = {
'original_topic': original_topic,
'original_key': key,
'original_value': value,
'error': error,
'timestamp': time.time()
}

self.dlq_producer.send(dlq_topic, value=dlq_message)
self.dlq_producer.flush()

def close(self):
self.producer.flush()
self.producer.close()
self.dlq_producer.close()


# WebSocket ingestion with production features
import websocket
import logging

class ProductionWebSocketCollector:
"""Enterprise-grade WebSocket collector"""

def __init__(self, ws_url, kafka_producer, topic):
self.ws_url = ws_url
self.kafka_producer = kafka_producer
self.topic = topic
self.logger = logging.getLogger(__name__)

# Metrics
self.messages_received = prom.Counter(
'websocket_messages_received_total',
'Total WebSocket messages',
['source']
)
self.connection_errors = prom.Counter(
'websocket_connection_errors_total',
'WebSocket connection errors',
['source']
)

def on_message(self, ws, message):
"""Handle incoming WebSocket message"""
try:
data = json.loads(message)

# Data validation
if not self.validate_message(data):
self.logger.warning(f"Invalid message: {message}")
return

# Enrich with metadata
enriched_data = self.enrich_message(data)

# Send to Kafka
key = enriched_data.get('symbol', '').encode('utf-8')
self.kafka_producer.send_message(
topic=self.topic,
key=key,
value=enriched_data,
headers=[
('source', 'websocket'),
('ingestion_time', str(time.time()))
]
)

self.messages_received.labels(source=self.ws_url).inc()

except Exception as e:
self.logger.error(f"Error processing message: {e}")
self.connection_errors.labels(source=self.ws_url).inc()

def on_error(self, ws, error):
"""Handle WebSocket error"""
self.logger.error(f"WebSocket error: {error}")
self.connection_errors.labels(source=self.ws_url).inc()

def on_close(self, ws, close_status_code, close_msg):
"""Handle WebSocket close"""
self.logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
# Implement reconnection logic
time.sleep(5)
self.connect()

def on_open(self, ws):
"""Handle WebSocket open"""
self.logger.info(f"WebSocket connected: {self.ws_url}")
# Subscribe to streams
subscribe_message = {
"method": "SUBSCRIBE",
"params": ["btcusdt@trade", "ethusdt@trade"],
"id": 1
}
ws.send(json.dumps(subscribe_message))

def validate_message(self, data):
"""Validate message structure"""
required_fields = ['symbol', 'price', 'timestamp']
return all(field in data for field in required_fields)

def enrich_message(self, data):
"""Enrich message with additional metadata"""
data['ingestion_timestamp'] = int(time.time() * 1000)
data['pipeline_version'] = '1.0.0'
return data

def connect(self):
"""Connect to WebSocket"""
self.ws = websocket.WebSocketApp(
self.ws_url,
on_message=self.on_message,
on_error=self.on_error,
on_close=self.on_close,
on_open=self.on_open
)
self.ws.run_forever()
```

---

## II. SPARK BATCH PROCESSING - ADVANCED IMPLEMENTATION

### 2.1 Production Spark Configuration

```python
"""
Enterprise Spark configuration for large-scale batch processing
"""
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_production_spark_session(app_name="CryptoAnalytics"):
"""Create optimized Spark session"""

conf = SparkConf()

# Application settings
conf.set("spark.app.name", app_name)
conf.set("spark.master", "k8s://https://kubernetes.default.svc:443")

# Resource allocation
conf.set("spark.executor.instances", "20")
conf.set("spark.executor.cores", "4")
conf.set("spark.executor.memory", "16g")
conf.set("spark.executor.memoryOverhead", "4g")
conf.set("spark.driver.memory", "8g")
conf.set("spark.driver.memoryOverhead", "2g")

# Shuffle optimization
conf.set("spark.sql.shuffle.partitions", "200")
conf.set("spark.shuffle.service.enabled", "true")
conf.set("spark.shuffle.compress", "true")
conf.set("spark.shuffle.spill.compress", "true")

# Adaptive Query Execution (Spark 3.x)
conf.set("spark.sql.adaptive.enabled", "true")
conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Cost-Based Optimization
conf.set("spark.sql.cbo.enabled", "true")
conf.set("spark.sql.cbo.joinReorder.enabled", "true")
conf.set("spark.sql.statistics.histogram.enabled", "true")

# Dynamic allocation
conf.set("spark.dynamicAllocation.enabled", "true")
conf.set("spark.dynamicAllocation.minExecutors", "5")
conf.set("spark.dynamicAllocation.maxExecutors", "50")
conf.set("spark.dynamicAllocation.initialExecutors", "10")

# Serialization
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryo.registrationRequired", "false")

# Broadcast
conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")

# Compression
conf.set("spark.sql.parquet.compression.codec", "snappy")
conf.set("spark.sql.orc.compression.codec", "zstd")

# Kubernetes specific
conf.set("spark.kubernetes.container.image", "spark:3.5.0-scala2.12")
conf.set("spark.kubernetes.namespace", "batch-processing")
conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")

# Monitoring
conf.set("spark.eventLog.enabled", "true")
conf.set("spark.eventLog.dir", "hdfs://namenode:9000/spark-events")
conf.set("spark.metrics.namespace", "crypto_analytics")

# Build session
spark = SparkSession.builder \
.config(conf=conf) \
.enableHiveSupport() \
.getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

return spark
```

### 2.2 Advanced Window Functions & Aggregations

```python
"""
Complex aggregations demonstrating intermediate-level Spark skills
"""
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

class AdvancedCryptoAnalytics:
"""Production-grade analytics implementation"""

def __init__(self, spark):
self.spark = spark

def calculate_advanced_technical_indicators(self, df_ohlcv):
"""
Calculate comprehensive technical indicators using window functions

Demonstrates:
- Complex window definitions
- Multiple aggregation functions
- Chained transformations
- Performance optimization
"""

# Define windows for different timeframes
window_symbol = Window.partitionBy("symbol").orderBy("timestamp")

# 7-period window
window_7 = window_symbol.rowsBetween(-6, 0)

# 14-period window
window_14 = window_symbol.rowsBetween(-13, 0)

# 20-period window
window_20 = window_symbol.rowsBetween(-19, 0)

# 50-period window
window_50 = window_symbol.rowsBetween(-49, 0)

# 200-period window
window_200 = window_symbol.rowsBetween(-199, 0)

df_indicators = df_ohlcv \
.withColumn("prev_close", lag("close", 1).over(window_symbol)) \
.withColumn("daily_return",
(col("close") - col("prev_close")) / col("prev_close")) \
\
.withColumn("sma_7", avg("close").over(window_7)) \
.withColumn("sma_14", avg("close").over(window_14)) \
.withColumn("sma_20", avg("close").over(window_20)) \
.withColumn("sma_50", avg("close").over(window_50)) \
.withColumn("sma_200", avg("close").over(window_200)) \
\
.withColumn("ema_12",
self.calculate_ema_udf(col("close"), lit(12)).over(window_symbol)) \
.withColumn("ema_26",
self.calculate_ema_udf(col("close"), lit(26)).over(window_symbol)) \
.withColumn("macd", col("ema_12") - col("ema_26")) \
.withColumn("signal_line",
self.calculate_ema_udf(col("macd"), lit(9)).over(window_symbol)) \
.withColumn("macd_histogram", col("macd") - col("signal_line")) \
\
.withColumn("stddev_20", stddev("close").over(window_20)) \
.withColumn("bollinger_middle", col("sma_20")) \
.withColumn("bollinger_upper", col("sma_20") + 2 * col("stddev_20")) \
.withColumn("bollinger_lower", col("sma_20") - 2 * col("stddev_20")) \
.withColumn("bollinger_width",
(col("bollinger_upper") - col("bollinger_lower")) / col("bollinger_middle")) \
\
.withColumn("typical_price", (col("high") + col("low") + col("close")) / 3) \
.withColumn("mfi", self.calculate_mfi_udf().over(window_14)) \
\
.withColumn("price_change", col("close") - lag("close", 1).over(window_symbol)) \
.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0)) \
.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0)) \
.withColumn("avg_gain", avg("gain").over(window_14)) \
.withColumn("avg_loss", avg("loss").over(window_14)) \
.withColumn("rs", col("avg_gain") / col("avg_loss")) \
.withColumn("rsi", 100 - (100 / (1 + col("rs")))) \
\
.withColumn("atr", self.calculate_atr_udf().over(window_14)) \
\
.withColumn("volume_sma_20", avg("volume").over(window_20)) \
.withColumn("volume_ratio", col("volume") / col("volume_sma_20")) \
\
.withColumn("price_momentum_7",
(col("close") - lag("close", 7).over(window_symbol)) / lag("close", 7).over(window_symbol) * 100) \
.withColumn("price_momentum_14",
(col("close") - lag("close", 14).over(window_symbol)) / lag("close", 14).over(window_symbol) * 100) \
\
.withColumn("volatility_7", stddev("daily_return").over(window_7) * sqrt(lit(365))) \
.withColumn("volatility_30", stddev("daily_return").over(window_symbol.rowsBetween(-29, 0)) * sqrt(lit(365)))

return df_indicators

def pivot_correlation_matrix(self, df_returns):
"""
Create correlation matrix using pivot operations

Demonstrates:
- Pivot operations
- Cross-joins
- Self-joins for correlation
"""

# Pivot to wide format
df_wide = df_returns.groupBy("timestamp") \
.pivot("symbol") \
.agg(first("daily_return"))

# Calculate correlation matrix
symbols = [col for col in df_wide.columns if col != "timestamp"]

correlation_matrix = {}
for sym1 in symbols:
correlation_matrix[sym1] = {}
for sym2 in symbols:
corr = df_wide.stat.corr(sym1, sym2)
correlation_matrix[sym1][sym2] = corr

return correlation_matrix

def calculate_rolling_beta(self, df_asset, df_market, window_days=30):
"""
Calculate rolling beta against market (e.g., BTC)

Demonstrates:
- Complex joins
- Window calculations
- Statistical functions
"""

window = Window.partitionBy("asset_symbol").orderBy("timestamp").rowsBetween(-window_days + 1, 0)

df_joined = df_asset.alias("asset") \
.join(
df_market.alias("market"),
(col("asset.timestamp") == col("market.timestamp")),
"inner"
) \
.select(
col("asset.symbol").alias("asset_symbol"),
col("asset.timestamp"),
col("asset.daily_return").alias("asset_return"),
col("market.daily_return").alias("market_return")
)

df_beta = df_joined \
.withColumn("cov_asset_market",
covar_pop("asset_return", "market_return").over(window)) \
.withColumn("var_market",
var_pop("market_return").over(window)) \
.withColumn("beta", col("cov_asset_market") / col("var_market")) \
.withColumn("alpha",
avg("asset_return").over(window) - col("beta") * avg("market_return").over(window))

return df_beta

def calculate_custom_risk_metrics(self, df_returns):
"""
Custom UDAF for portfolio risk metrics

Demonstrates:
- Pandas UDF (vectorized UDF)
- Custom aggregation logic
- Statistical computations
"""

@pandas_udf("double")
def calculate_value_at_risk(returns: pd.Series) -> float:
"""Calculate 95% Value at Risk"""
if len(returns) < 30:
return None
return float(returns.quantile(0.05))

@pandas_udf("double")
def calculate_conditional_var(returns: pd.Series) -> float:
"""Calculate Conditional VaR (Expected Shortfall)"""
if len(returns) < 30:
return None
var_95 = returns.quantile(0.05)
return float(returns[returns <= var_95].mean())

@pandas_udf("double")
def calculate_sharpe_ratio(returns: pd.Series) -> float:
"""Calculate annualized Sharpe Ratio"""
if len(returns) < 30 or returns.std() == 0:
return None
return float((returns.mean() / returns.std()) * np.sqrt(365))

@pandas_udf("double")
def calculate_sortino_ratio(returns: pd.Series) -> float:
"""Calculate Sortino Ratio (downside deviation)"""
if len(returns) < 30:
return None
downside_returns = returns[returns < 0]
if len(downside_returns) == 0 or downside_returns.std() == 0:
return None
return float((returns.mean() / downside_returns.std()) * np.sqrt(365))

@pandas_udf("double")
def calculate_max_drawdown(prices: pd.Series) -> float:
"""Calculate maximum drawdown"""
if len(prices) < 2:
return None
cummax = prices.expanding().max()
drawdown = (prices - cummax) / cummax
return float(drawdown.min())

df_risk = df_returns.groupBy("symbol").agg(
calculate_value_at_risk(col("daily_return")).alias("var_95"),
calculate_conditional_var(col("daily_return")).alias("cvar_95"),
calculate_sharpe_ratio(col("daily_return")).alias("sharpe_ratio"),
calculate_sortino_ratio(col("daily_return")).alias("sortino_ratio"),
calculate_max_drawdown(col("close")).alias("max_drawdown"),
avg("daily_return").alias("avg_return"),
stddev("daily_return").alias("volatility"),
min("close").alias("min_price"),
max("close").alias("max_price")
)

return df_risk

```

Tôi sẽ tiếp tục tạo các file technical deep dive chi tiết. Bạn có muốn tôi:

1. Tiếp tục phần còn lại của TECHNICAL_DEEP_DIVE.md (Spark Streaming, ML, GraphFrames)?
2. Tạo file Kubernetes deployment manifests?
3. Tạo file về Data Quality & Governance?
4. Hoặc phần nào khác bạn muốn ưu tiên?
