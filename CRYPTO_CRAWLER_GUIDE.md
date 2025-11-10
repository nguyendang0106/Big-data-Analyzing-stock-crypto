# 🚀 Hướng Dẫn Sử Dụng Cryptocurrency Data Pipeline

## Integration với Apache Airflow & Big Data Stack

## 📦 Cài Đặt

### Bước 1: Cài đặt Python packages

```bash
cd "/Users/nguyentiendang0106/Documents/20251/Big data"
pip install -r requirements.txt
```

Hoặc cài từng package:

```bash
pip install requests pandas numpy schedule openpyxl apache-airflow pyspark kafka-python
```

### Bước 2: Cài đặt Big Data Stack (Optional - For Production)

```bash
# Airflow
pip install apache-airflow apache-airflow-providers-apache-spark

# Kafka Python Client
pip install kafka-python confluent-kafka

# Spark
# Download từ https://spark.apache.org/downloads.html
```

## 🎯 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   DATA PIPELINE FLOW                         │
└─────────────────────────────────────────────────────────────┘

    Crawlers (Python)
         │
         ├── test.py (Basic)
         ├── crypto_crawler_advanced.py (Advanced)
         │
         ▼
    Apache Kafka Topics
         │
         ├── crypto.trades.raw
         ├── crypto.ohlcv.1m
         ├── crypto.market.data
         │
         ▼
    ┌────────────────────────────────┐
    │   Apache Airflow DAGs          │
    │   (Orchestration)              │
    └────────────────────────────────┘
         │
         ├─► Batch Processing (Spark)
         │   - Daily aggregations
         │   - Feature engineering
         │   - ML model training
         │
         ├─► Stream Processing (Spark)
         │   - Real-time analytics
         │   - Anomaly detection
         │   - Live predictions
         │
         ▼
    Storage Layer
         │
         ├── HDFS (Batch data)
         ├── HBase (Batch views)
         └── Redis (Real-time views)
         │
         ▼
    Serving Layer (API + Dashboard)
```

## 🧪 Testing Strategy

### Unit Tests

```bash
# Test individual crawlers
pytest tests/test_crawlers.py

# Test Airflow DAGs
pytest tests/test_dags.py

# Test Kafka integration
pytest tests/test_kafka_integration.py
```

### Integration Tests

```bash
# Test full ETL pipeline
airflow dags test crypto_etl_dag 2025-11-10

# Test specific task
airflow tasks test crypto_etl_dag crawl_coingecko 2025-11-10
```

### Load Testing

```bash
# Simulate high throughput
python scripts/load_test_kafka.py --rate 10000 --duration 300
```

---

## 📈 Monitoring & Observability

### Metrics to Track

**Crawler Metrics:**

- Requests per second
- Success rate
- Response time
- Error rate

**Airflow Metrics:**

- DAG success rate
- Task duration
- Queue size
- Worker utilization

**Kafka Metrics:**

- Message throughput
- Consumer lag
- Partition distribution
- Disk usage

**Data Quality Metrics:**

- Missing values %
- Duplicate records
- Schema violations
- Freshness (time since last update)

### Monitoring Tools

```bash
# Airflow metrics
http://localhost:8080/health

# Prometheus metrics endpoint
http://localhost:9090

# Grafana dashboards
http://localhost:3000
```

---

## 🚨 Troubleshooting

### Common Issues

#### 1. Airflow DAG not appearing

```bash
# Check DAG file syntax
python airflow/dags/crypto_etl_dag.py

# Refresh DAGs
airflow dags list-import-errors
```

#### 2. Kafka connection failed

```bash
# Check Kafka status
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Test connectivity
telnet localhost 9092
```

#### 3. Rate limit exceeded

```python
# Add exponential backoff
from tenacity import retry, wait_exponential

@retry(wait=wait_exponential(multiplier=1, min=4, max=60))
def crawl_with_retry():
    return crawler.get_data()
```

#### 4. Memory issues in Spark

```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")

# Repartition data
df = df.repartition(200)
```

#### 5. Airflow task stuck in running

```bash
# Kill zombie tasks
airflow tasks clear crypto_etl_dag --task-regex 'crawl.*' --start-date 2025-11-10

# Restart scheduler
pkill -f "airflow scheduler"
airflow scheduler &
```

---

## 📊 Performance Optimization

### Crawler Optimization

```python
# Use async/await for concurrent requests
import asyncio
import aiohttp

async def crawl_async(symbols):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_price(session, symbol) for symbol in symbols]
        return await asyncio.gather(*tasks)

# Result: 10x faster than sequential
```

### Airflow Optimization

```python
# Enable parallelism
AIRFLOW__CORE__PARALLELISM = 32
AIRFLOW__CORE__DAG_CONCURRENCY = 16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG = 3

# Use connection pooling
AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE = 10
```

### Kafka Optimization

```yaml
# Producer config
batch.size: 32768
linger.ms: 10
compression.type: lz4

# Consumer config
fetch.min.bytes: 1024
fetch.max.wait.ms: 500
```

---

## � Best Practices

### 1. Idempotent DAGs

```python
# DAGs should be rerunnable without side effects
# Use upsert instead of insert
# Check if data already exists before processing
```

### 2. Data Versioning

```python
# Include version in file names
output_path = f"hdfs:///crypto/ohlcv/v1/{date}/"

# Track schema versions
schema_version = "1.0.0"
```

### 3. Error Handling

```python
# Always log errors with context
try:
    result = crawler.get_data()
except Exception as e:
    logger.error(f"Failed to crawl {symbol}: {e}", exc_info=True)
    send_alert(f"Crawler failed: {symbol}")
    raise
```

### 4. Resource Management

```python
# Always close connections
try:
    producer = KafkaProducer(...)
    # Use producer
finally:
    producer.flush()
    producer.close()
```

### 5. Configuration Management

```python
# Use environment variables
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')

# Never hardcode credentials
API_KEY = os.getenv('COINGECKO_API_KEY')
```

---

## 🔐 Security Considerations

### API Keys

```bash
# Use Airflow Connections for secrets
airflow connections add 'coingecko_api' \
    --conn-type 'http' \
    --conn-password 'your_api_key'

# Access in DAG
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('coingecko_api')
api_key = conn.password
```

### Network Security

```yaml
# Use TLS for Kafka
security.protocol: SSL
ssl.truststore.location: /path/to/truststore.jks
```

### Data Privacy

```python
# Anonymize sensitive data
df = df.withColumn('user_id', hash_udf('user_id'))
```

---

## 📚 Additional Resources

### Documentation

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

### Monitoring Dashboards

- Airflow UI: http://localhost:8080
- Kafka Manager: http://localhost:9000
- Grafana: http://localhost:3000

### Sample Queries

```python
# Query from HBase
from happybase import Connection

conn = Connection('localhost')
table = conn.table('crypto_ohlcv')

# Scan recent data
for key, data in table.scan(row_prefix=b'BTC_'):
    print(key, data)
```

---

## 📝 Changelog

### Version 2.0.0 (2025-11-10)

- ✅ Added Airflow integration
- ✅ Added Kafka streaming support
- ✅ Added 3 production DAGs
- ✅ Enhanced monitoring and logging
- ✅ Added comprehensive testing guide

### Version 1.0.0 (2025-10-20)

- ✅ Initial release
- ✅ Basic and advanced crawlers
- ✅ Standalone mode support

---

**Ready to Deploy! 🚀**

Start with standalone mode for development, then migrate to Airflow orchestration for production.

### Mode 1: Standalone Mode (Development)

#### File 1: `test.py` - Basic Crawler

Crawler cơ bản với CoinGecko API cho development và testing:

```bash
cd data
python test.py
```

**Tính năng:**

- ✅ Crawl top 50 cryptocurrencies
- ✅ Crawl trending coins
- ✅ Crawl dữ liệu lịch sử 30 ngày (Bitcoin, Ethereum)
- ✅ Crawl global market data
- ✅ Crawl thông tin chi tiết Bitcoin

**Output:** Tạo thư mục `crypto_data/` với các file CSV và JSON

**Use Case:** Quick testing, data exploration, prototyping

---

#### File 2: `crypto_crawler_advanced.py` - Advanced Crawler

Crawler nâng cao với nhiều nguồn dữ liệu:

```bash
cd data
python crypto_crawler_advanced.py
```

**Menu chức năng:**

```
1. Crawl Top 100 Cryptocurrencies (CoinGecko)
2. Crawl DeFi Protocols
3. Crawl Binance 24h Ticker
4. Crawl Bitcoin Candlestick + Technical Indicators
5. Crawl Bitcoin Orderbook
6. Crawl tất cả (One-time)
7. Chạy Scheduled Crawler (Auto crawl mỗi 60 phút)
```

**Tính năng nâng cao:**

- ✅ Crawl từ CoinGecko và Binance
- ✅ Tính toán technical indicators (SMA, EMA, MACD, RSI, Bollinger Bands)
- ✅ Phân tích market sentiment
- ✅ Crawl orderbook depth
- ✅ Scheduled tasks (tự động crawl định kỳ)
- ✅ Logging system

**Use Case:** Production-ready crawler, continuous data collection

---

### Mode 2: Airflow Orchestration Mode (Production)

#### Setup Airflow

```bash
# Initialize Airflow database
export AIRFLOW_HOME=~/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start Airflow webserver
airflow webserver --port 8080 &

# Start Airflow scheduler
airflow scheduler &
```

#### DAG 1: `crypto_etl_dag.py` - Daily ETL Pipeline

**Purpose:** Automated daily data collection and processing

**Schedule:** Runs daily at 2:00 AM

**Tasks Flow:**

```python
start → crawl_coingecko → crawl_binance →
validate_data → publish_to_kafka →
load_to_hdfs → trigger_spark_batch →
save_to_hbase → update_dashboard → end
```

**Manual Trigger:**

```bash
# Trigger DAG manually
airflow dags trigger crypto_etl_dag

# Check DAG status
airflow dags list

# View task logs
airflow tasks logs crypto_etl_dag crawl_coingecko 2025-11-10
```

**Configuration:**

```python
# airflow/dags/crypto_etl_dag.py
default_args = {
    'owner': 'crypto_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_etl_dag',
    default_args=default_args,
    description='Daily cryptocurrency ETL pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['crypto', 'etl', 'daily'],
)
```

---

#### DAG 2: `ml_pipeline_dag.py` - ML Training Pipeline

**Purpose:** Weekly model training and evaluation

**Schedule:** Runs every Sunday at 3:00 AM

**Tasks Flow:**

```python
start → extract_features →
feature_engineering → train_price_predictor →
train_anomaly_detector → evaluate_models →
deploy_best_model → update_model_registry →
send_report → end
```

**Manual Trigger:**

```bash
airflow dags trigger ml_pipeline_dag

# Backfill for specific date range
airflow dags backfill ml_pipeline_dag \
    --start-date 2025-10-01 \
    --end-date 2025-10-31
```

**Configuration:**

```python
dag = DAG(
    'ml_pipeline_dag',
    default_args=default_args,
    description='Weekly ML model training and deployment',
    schedule_interval='0 3 * * 0',  # Weekly on Sunday at 3 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['crypto', 'ml', 'training'],
)
```

---

#### DAG 3: `reporting_dag.py` - Analytics & Reporting

**Purpose:** Generate daily reports and metrics

**Schedule:** Runs daily at 8:00 AM

**Tasks Flow:**

```python
start → aggregate_daily_stats →
calculate_portfolio_metrics →
compute_risk_metrics → check_alerts →
generate_report → send_email →
update_dashboard_db → end
```

**Manual Trigger:**

```bash
airflow dags trigger reporting_dag
```

---

### Mode 3: Kafka Integration Mode (Real-time)

#### Kafka Producer Integration

```python
"""
Integrate crawler with Kafka for real-time streaming
"""
from kafka import KafkaProducer
import json

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send data to Kafka topic
def send_to_kafka(data, topic='crypto.ohlcv.1m'):
    try:
        future = producer.send(topic, value=data)
        record_metadata = future.get(timeout=10)
        print(f"Sent to {record_metadata.topic} partition {record_metadata.partition}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

# Example: Crawl and stream to Kafka
from crypto_crawler_advanced import AdvancedCryptoCrawler

crawler = AdvancedCryptoCrawler()
df = crawler.crawl_binance_klines('BTCUSDT', interval='1m', limit=1)

# Convert to dict and send
for row in df.to_dict('records'):
    send_to_kafka(row, topic='crypto.ohlcv.1m')
```

#### Run Continuous Streaming

```bash
# Run crawler with Kafka output
python scripts/kafka_streaming_producer.py --symbols BTC,ETH,BNB --interval 1m
```

## 📊 Dữ Liệu Thu Thập Được

### CoinGecko Data

- **Market Data:** Giá hiện tại, market cap, volume, price changes
- **Historical Data:** Giá lịch sử theo ngày/giờ
- **Trending Coins:** Top trending cryptocurrencies
- **Global Data:** Tổng market cap, volume toàn thị trường
- **Coin Details:** Thông tin chi tiết về từng coin

### Binance Data

- **24h Ticker:** Thống kê 24h của tất cả trading pairs
- **Candlestick (Klines):** OHLCV data với nhiều timeframes
- **Orderbook:** Bid/Ask depth data
- **Technical Indicators:** SMA, EMA, MACD, RSI, Bollinger Bands

## � Airflow Web UI

### Access Dashboard

```bash
# Open browser
http://localhost:8080

# Login credentials
Username: admin
Password: admin
```

### Monitor DAGs

1. **DAGs View**: See all DAGs and their schedules
2. **Graph View**: Visualize task dependencies
3. **Gantt View**: Analyze task execution timeline
4. **Tree View**: Historical runs overview
5. **Logs View**: Debug task failures

### Key Metrics to Monitor

- ✅ DAG success rate
- ✅ Task duration
- ✅ Data volume processed
- ✅ Retry attempts
- ✅ Failed tasks

---

## 🔄 Complete Workflow Example

### Scenario: Daily Production Pipeline

**Time: 2:00 AM** - Airflow triggers `crypto_etl_dag`

```
1. Task: crawl_coingecko
   - Crawls top 100 coins
   - Saves to temp storage
   Duration: 5 minutes

2. Task: crawl_binance
   - Crawls OHLCV data for 100 symbols
   - Saves to temp storage
   Duration: 10 minutes

3. Task: validate_data
   - Schema validation
   - Data quality checks
   - Remove duplicates
   Duration: 2 minutes

4. Task: publish_to_kafka
   - Send validated data to Kafka topics
   - crypto.ohlcv.1m
   - crypto.market.data
   Duration: 3 minutes

5. Task: load_to_hdfs
   - Batch load from Kafka to HDFS
   - Partition by date and symbol
   Duration: 15 minutes

6. Task: trigger_spark_batch
   - Submit Spark job for aggregations
   - Calculate technical indicators
   - Feature engineering
   Duration: 30 minutes

7. Task: save_to_hbase
   - Save batch views to HBase
   Duration: 10 minutes

8. Task: update_dashboard
   - Refresh dashboard metrics
   Duration: 2 minutes

Total Pipeline Duration: ~77 minutes
```

**Time: 8:00 AM** - Airflow triggers `reporting_dag`

```
Generate daily reports with fresh data
Send email alerts if anomalies detected
```

**Time: 3:00 AM (Sunday)** - Airflow triggers `ml_pipeline_dag`

```
Train ML models with past week's data
Deploy new models if performance improved
```

---

## 🛠️ Advanced Airflow Features

### 1. Dynamic DAG Generation

```python
# Generate DAGs for multiple symbols
SYMBOLS = ['BTC', 'ETH', 'BNB', 'ADA', 'SOL']

for symbol in SYMBOLS:
    dag_id = f'crypto_etl_{symbol.lower()}'

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval='*/15 * * * *',  # Every 15 minutes
        tags=['crypto', symbol]
    )

    # Define tasks...
    globals()[dag_id] = dag
```

### 2. XComs for Data Passing

```python
# Task 1: Crawl data and push to XCom
def crawl_data(**context):
    data = crawler.get_top_cryptocurrencies(100)
    context['task_instance'].xcom_push(key='crypto_data', value=data)

# Task 2: Pull from XCom and process
def process_data(**context):
    data = context['task_instance'].xcom_pull(key='crypto_data')
    # Process data...
```

### 3. Sensors for External Dependencies

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_data_file',
    filepath='/data/crypto/latest.csv',
    poke_interval=60,
    timeout=3600,
    dag=dag
)
```

### 4. Branching for Conditional Logic

```python
from airflow.operators.python import BranchPythonOperator

def decide_processing(**context):
    data_size = context['task_instance'].xcom_pull(key='data_size')
    if data_size > 1000000:
        return 'heavy_processing'
    else:
        return 'light_processing'

branch = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_processing,
    dag=dag
)
```

### 5. Callbacks for Notifications

```python
def on_failure_callback(context):
    """Send Slack/Email notification on failure"""
    send_alert(f"DAG {context['dag'].dag_id} failed!")

dag = DAG(
    'crypto_etl_dag',
    default_args={
        'on_failure_callback': on_failure_callback,
    }
)
```

---

## �🔧 Tùy Chỉnh

### Crawler Configuration

```python
# config/crawler_config.yaml
sources:
  coingecko:
    enabled: true
    symbols: 100
    rate_limit: 50  # requests per minute

  binance:
    enabled: true
    symbols: ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    intervals: ['1m', '5m', '1h', '1d']

storage:
  output_format: 'parquet'  # csv, json, parquet
  compression: 'snappy'

kafka:
  enabled: true
  bootstrap_servers: 'localhost:9092'
  topics:
    ohlcv: 'crypto.ohlcv.1m'
    trades: 'crypto.trades.raw'
```

### Thay đổi số lượng coins crawl:

```python
# Trong test.py hoặc crypto_crawler_advanced.py
top_coins = crawler.get_top_cryptocurrencies(limit=200)  # Thay đổi từ 50 -> 200
```

### Thay đổi timeframe dữ liệu lịch sử:

```python
# Crawl 90 ngày thay vì 30 ngày
historical = crawler.get_historical_data('bitcoin', days=90)
```

### Airflow DAG Schedule Customization:

```python
# Chạy mỗi 15 phút
schedule_interval='*/15 * * * *'

# Chạy mỗi giờ
schedule_interval='0 * * * *'

# Chạy mỗi ngày lúc 2:30 AM
schedule_interval='30 2 * * *'

# Chạy mỗi thứ 2 lúc 8:00 AM
schedule_interval='0 8 * * 1'
```

### Thay đổi trading pair trên Binance:

```python
# Crawl Ethereum thay vì Bitcoin
df = crawler.crawl_binance_klines('ETHUSDT', interval='1h', limit=100)
```

### Thay đổi interval cho scheduled crawler:

```python
# Chạy mỗi 30 phút thay vì 60 phút
crawler.run_scheduler(interval_minutes=30)
```

## 📈 Phân Tích Dữ Liệu

### Load và phân tích dữ liệu đã crawl:

```python
import pandas as pd

# Load data
df = pd.read_csv('crypto_data_advanced/coingecko_markets_YYYYMMDD_HHMMSS.csv')

# Top 10 gainers 24h
top_gainers = df.nlargest(10, 'price_change_percentage_24h')
print(top_gainers[['name', 'symbol', 'current_price', 'price_change_percentage_24h']])

# Top 10 losers 24h
top_losers = df.nsmallest(10, 'price_change_percentage_24h')
print(top_losers[['name', 'symbol', 'current_price', 'price_change_percentage_24h']])

# Coins with RSI > 70 (overbought)
btc_data = pd.read_csv('crypto_data_advanced/btc_klines_with_indicators_YYYYMMDD_HHMMSS.csv')
overbought = btc_data[btc_data['RSI'] > 70]
print(f"Overbought periods: {len(overbought)}")
```

## 🔄 Scheduled Crawling

### Chạy crawler tự động mỗi giờ:

```python
from crypto_crawler_advanced import AdvancedCryptoCrawler

crawler = AdvancedCryptoCrawler()
crawler.run_scheduler(interval_minutes=60)  # Crawl mỗi 60 phút
```

### Hoặc sử dụng cron job (Linux/macOS):

```bash
# Chỉnh sửa crontab
crontab -e

# Thêm dòng này để chạy mỗi giờ
0 * * * * cd /path/to/project/data && /usr/bin/python3 crypto_crawler_advanced.py
```

## 📝 Logging

Logs được lưu trong file `crypto_crawler.log`:

```bash
# Xem logs real-time
tail -f crypto_crawler.log

# Xem 100 dòng cuối
tail -n 100 crypto_crawler.log
```

## ⚠️ Lưu Ý Quan Trọng

### Rate Limits

**CoinGecko Free API:**

- ~10-50 requests/phút
- Sử dụng `time.sleep()` giữa các requests

**Binance API:**

- 1200 requests/phút cho IP
- 6000 requests/phút cho UID

### Best Practices

1. **Luôn check response status:**

   ```python
   response.raise_for_status()
   ```

2. **Sử dụng timeout:**

   ```python
   requests.get(url, timeout=30)
   ```

3. **Handle exceptions:**

   ```python
   try:
       data = crawler.crawl_data()
   except Exception as e:
       logger.error(f"Error: {e}")
   ```

4. **Rate limiting:**
   ```python
   time.sleep(2)  # Delay 2 giây giữa các requests
   ```

## 🎓 Examples

### Example 1: Crawl và analyze Bitcoin

```python
from crypto_crawler_advanced import AdvancedCryptoCrawler

crawler = AdvancedCryptoCrawler()

# Crawl Bitcoin data
df = crawler.crawl_binance_klines('BTCUSDT', interval='1d', limit=365)

# Calculate indicators
df_with_indicators = crawler.calculate_technical_indicators(df)

# Save
crawler.save_data(df_with_indicators, 'btc_yearly_analysis')

# Analyze
print(f"Current RSI: {df_with_indicators['RSI'].iloc[-1]:.2f}")
print(f"MACD: {df_with_indicators['MACD'].iloc[-1]:.2f}")
```

### Example 2: Monitor market sentiment

```python
crawler = AdvancedCryptoCrawler()

# Get market data
df = crawler.crawl_coingecko_markets(limit=100)

# Analyze sentiment
sentiment = crawler.analyze_market_sentiment(df)

print(f"Gainers: {sentiment['gainers']}")
print(f"Losers: {sentiment['losers']}")
print(f"Average 24h change: {sentiment['avg_change_24h']:.2f}%")
```

### Example 3: Compare multiple coins

```python
coins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT']

for coin in coins:
    df = crawler.crawl_binance_klines(coin, interval='1h', limit=24)
    if df is not None:
        price_change = ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]) * 100
        print(f"{coin}: {price_change:+.2f}% (24h)")
    time.sleep(1)
```

## 🐛 Troubleshooting

### Lỗi: Module not found

```bash
pip install requests pandas numpy schedule
```

### Lỗi: Rate limit exceeded

```python
# Tăng delay giữa các requests
time.sleep(5)  # Thay vì 2 giây
```

### Lỗi: Connection timeout

```python
# Tăng timeout
response = requests.get(url, timeout=60)  # Thay vì 30
```

## 📚 Tài Liệu API

- [CoinGecko API Docs](https://www.coingecko.com/en/api/documentation)
- [Binance API Docs](https://binance-docs.github.io/apidocs/spot/en/)

## 💡 Tips & Tricks

1. **Lưu dữ liệu với timestamp** để không bị ghi đè
2. **Sử dụng parquet format** cho files lớn (nhanh hơn CSV)
3. **Crawl theo batch** để tránh rate limit
4. **Backup dữ liệu định kỳ**
5. **Monitor disk space** khi crawl liên tục

## 🚀 Next Steps

- [ ] Tích hợp thêm exchanges (Coinbase, Kraken, FTX)
- [ ] Thêm real-time WebSocket streaming
- [ ] Xây dựng database (PostgreSQL/MongoDB)
- [ ] Tạo dashboard visualization (Streamlit/Dash)
- [ ] Machine Learning models cho price prediction
- [ ] Alert system cho price movements

---

**Happy Crawling! 🎉**
