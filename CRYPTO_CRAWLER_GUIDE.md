# 🚀 Hướng Dẫn Sử Dụng Cryptocurrency Crawler

## 📦 Cài Đặt

### Bước 1: Cài đặt Python packages

```bash
cd "..."
pip install -r requirements.txt
```

Hoặc cài từng package:

```bash
pip install requests pandas numpy schedule openpyxl
```

## 🎯 Cách Sử Dụng

### File 1: `test.py` - Basic Crawler

Crawler cơ bản với CoinGecko API:

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

### File 2: `crypto_crawler_advanced.py` - Advanced Crawler

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

## 🔧 Tùy Chỉnh

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
