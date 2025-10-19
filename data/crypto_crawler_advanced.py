"""
Advanced Cryptocurrency Crawler với Real-time Data và Scheduled Tasks
Hỗ trợ crawl từ nhiều nguồn: CoinGecko, Binance, CoinMarketCap
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import schedule
from threading import Thread
import logging
from pathlib import Path


# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AdvancedCryptoCrawler:
    """Advanced Cryptocurrency Crawler với nhiều nguồn dữ liệu"""
    
    def __init__(self, output_dir='crypto_data'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # CoinGecko API
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        
        # Binance API (không cần API key cho public endpoints)
        self.binance_base = "https://api.binance.com/api/v3"
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        }
        
        logger.info("✅ Initialized AdvancedCryptoCrawler")
    
    # ============ COINGECKO METHODS ============
    
    def crawl_coingecko_markets(self, limit=100, vs_currency='usd'):
        """Crawl market data từ CoinGecko"""
        try:
            url = f"{self.coingecko_base}/coins/markets"
            params = {
                'vs_currency': vs_currency,
                'order': 'market_cap_desc',
                'per_page': limit,
                'page': 1,
                'sparkline': False,
                'price_change_percentage': '1h,24h,7d,30d'
            }
            
            response = requests.get(url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            df = pd.DataFrame(data)
            df['source'] = 'coingecko'
            df['crawled_at'] = datetime.now()
            
            logger.info(f"✅ Crawled {len(df)} coins from CoinGecko")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error crawling CoinGecko: {e}")
            return None
    
    def crawl_defi_protocols(self):
        """Crawl DeFi protocols data"""
        try:
            url = f"{self.coingecko_base}/coins/markets"
            params = {
                'vs_currency': 'usd',
                'category': 'decentralized-finance-defi',
                'order': 'market_cap_desc',
                'per_page': 50,
                'page': 1
            }
            
            response = requests.get(url, params=params, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            df = pd.DataFrame(data)
            df['category'] = 'DeFi'
            df['crawled_at'] = datetime.now()
            
            logger.info(f"✅ Crawled {len(df)} DeFi protocols")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error crawling DeFi protocols: {e}")
            return None
    
    # ============ BINANCE METHODS ============
    
    def crawl_binance_ticker(self):
        """Crawl 24h ticker price change từ Binance"""
        try:
            url = f"{self.binance_base}/ticker/24hr"
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Filter USDT pairs
            usdt_pairs = [d for d in data if d['symbol'].endswith('USDT')]
            
            df = pd.DataFrame(usdt_pairs)
            df['source'] = 'binance'
            df['crawled_at'] = datetime.now()
            
            # Convert string to numeric
            numeric_cols = ['lastPrice', 'priceChange', 'priceChangePercent', 
                          'volume', 'quoteVolume', 'highPrice', 'lowPrice']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            logger.info(f"✅ Crawled {len(df)} pairs from Binance")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error crawling Binance: {e}")
            return None
    
    def crawl_binance_klines(self, symbol='BTCUSDT', interval='1h', limit=100):
        """
        Crawl candlestick data từ Binance
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Kline interval (1m, 5m, 15m, 1h, 4h, 1d)
            limit: Number of candles (max 1000)
        """
        try:
            url = f"{self.binance_base}/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Convert timestamps
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            # Convert to numeric
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col])
            
            df['symbol'] = symbol
            df['interval'] = interval
            
            logger.info(f"✅ Crawled {len(df)} klines for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error crawling Binance klines: {e}")
            return None
    
    def crawl_binance_orderbook(self, symbol='BTCUSDT', limit=20):
        """Crawl order book depth từ Binance"""
        try:
            url = f"{self.binance_base}/depth"
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            bids = pd.DataFrame(data['bids'], columns=['price', 'quantity'])
            asks = pd.DataFrame(data['asks'], columns=['price', 'quantity'])
            
            bids['type'] = 'bid'
            asks['type'] = 'ask'
            
            df = pd.concat([bids, asks], ignore_index=True)
            df['symbol'] = symbol
            df['crawled_at'] = datetime.now()
            
            # Convert to numeric
            df['price'] = pd.to_numeric(df['price'])
            df['quantity'] = pd.to_numeric(df['quantity'])
            
            logger.info(f"✅ Crawled orderbook for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error crawling orderbook: {e}")
            return None
    
    # ============ DATA ANALYSIS ============
    
    def analyze_market_sentiment(self, df):
        """Phân tích sentiment thị trường"""
        if df is None or df.empty:
            return None
        
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'total_coins': len(df),
            'gainers': len(df[df['price_change_percentage_24h'] > 0]),
            'losers': len(df[df['price_change_percentage_24h'] < 0]),
            'avg_change_24h': df['price_change_percentage_24h'].mean(),
            'top_gainer': df.nlargest(1, 'price_change_percentage_24h')[['name', 'symbol', 'price_change_percentage_24h']].to_dict('records'),
            'top_loser': df.nsmallest(1, 'price_change_percentage_24h')[['name', 'symbol', 'price_change_percentage_24h']].to_dict('records'),
            'total_market_cap': df['market_cap'].sum(),
            'total_volume_24h': df['total_volume'].sum()
        }
        
        return analysis
    
    def calculate_technical_indicators(self, df):
        """Tính toán các chỉ số kỹ thuật"""
        if df is None or df.empty or 'close' not in df.columns:
            return df
        
        # Simple Moving Averages
        df['SMA_7'] = df['close'].rolling(window=7).mean()
        df['SMA_25'] = df['close'].rolling(window=25).mean()
        df['SMA_99'] = df['close'].rolling(window=99).mean()
        
        # Exponential Moving Average
        df['EMA_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['EMA_26'] = df['close'].ewm(span=26, adjust=False).mean()
        
        # MACD
        df['MACD'] = df['EMA_12'] - df['EMA_26']
        df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
        
        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        df['BB_Middle'] = df['close'].rolling(window=20).mean()
        bb_std = df['close'].rolling(window=20).std()
        df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
        df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
        
        logger.info("✅ Calculated technical indicators")
        return df
    
    # ============ SAVE METHODS ============
    
    def save_data(self, df, filename_prefix, format='csv'):
        """Lưu dữ liệu với timestamp"""
        if df is None or df.empty:
            logger.warning("⚠️ No data to save")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == 'csv':
            filepath = self.output_dir / f"{filename_prefix}_{timestamp}.csv"
            df.to_csv(filepath, index=False)
        elif format == 'json':
            filepath = self.output_dir / f"{filename_prefix}_{timestamp}.json"
            df.to_json(filepath, orient='records', indent=2)
        elif format == 'parquet':
            filepath = self.output_dir / f"{filename_prefix}_{timestamp}.parquet"
            df.to_parquet(filepath, index=False)
        
        logger.info(f"💾 Saved to {filepath}")
        return filepath
    
    # ============ SCHEDULED TASKS ============
    
    def scheduled_crawl_job(self):
        """Job crawl định kỳ"""
        logger.info("🔄 Starting scheduled crawl job...")
        
        # Crawl CoinGecko markets
        df_markets = self.crawl_coingecko_markets(limit=100)
        if df_markets is not None:
            self.save_data(df_markets, 'coingecko_markets')
            
            # Analyze sentiment
            sentiment = self.analyze_market_sentiment(df_markets)
            if sentiment:
                filepath = self.output_dir / f"sentiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filepath, 'w') as f:
                    json.dump(sentiment, f, indent=2)
        
        time.sleep(5)
        
        # Crawl Binance ticker
        df_binance = self.crawl_binance_ticker()
        if df_binance is not None:
            self.save_data(df_binance, 'binance_ticker')
        
        time.sleep(5)
        
        # Crawl Bitcoin klines
        df_btc = self.crawl_binance_klines('BTCUSDT', interval='1h', limit=168)  # 1 week
        if df_btc is not None:
            df_btc_with_indicators = self.calculate_technical_indicators(df_btc)
            self.save_data(df_btc_with_indicators, 'btc_klines_with_indicators')
        
        logger.info("✅ Scheduled crawl job completed")
    
    def run_scheduler(self, interval_minutes=60):
        """Chạy crawler theo lịch"""
        logger.info(f"⏰ Starting scheduler (interval: {interval_minutes} minutes)")
        
        # Schedule job
        schedule.every(interval_minutes).minutes.do(self.scheduled_crawl_job)
        
        # Run immediately
        self.scheduled_crawl_job()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(1)


def main():
    """Main function"""
    print("=" * 80)
    print("🚀 ADVANCED CRYPTOCURRENCY CRAWLER")
    print("=" * 80)
    
    crawler = AdvancedCryptoCrawler(output_dir='crypto_data_advanced')
    
    # Menu
    print("\n📋 Chọn chức năng:")
    print("1. Crawl Top 100 Cryptocurrencies (CoinGecko)")
    print("2. Crawl DeFi Protocols")
    print("3. Crawl Binance 24h Ticker")
    print("4. Crawl Bitcoin Candlestick + Technical Indicators")
    print("5. Crawl Bitcoin Orderbook")
    print("6. Crawl tất cả (One-time)")
    print("7. Chạy Scheduled Crawler (Auto crawl mỗi 60 phút)")
    
    choice = input("\nNhập lựa chọn (1-7): ").strip()
    
    if choice == '1':
        df = crawler.crawl_coingecko_markets(limit=100)
        crawler.save_data(df, 'coingecko_top100')
        
    elif choice == '2':
        df = crawler.crawl_defi_protocols()
        crawler.save_data(df, 'defi_protocols')
        
    elif choice == '3':
        df = crawler.crawl_binance_ticker()
        crawler.save_data(df, 'binance_ticker')
        
    elif choice == '4':
        df = crawler.crawl_binance_klines('BTCUSDT', interval='1h', limit=168)
        df_with_indicators = crawler.calculate_technical_indicators(df)
        crawler.save_data(df_with_indicators, 'btc_hourly_indicators')
        print("\n📊 Sample data với technical indicators:")
        print(df_with_indicators[['open_time', 'close', 'SMA_7', 'RSI', 'MACD']].tail(10))
        
    elif choice == '5':
        df = crawler.crawl_binance_orderbook('BTCUSDT', limit=50)
        crawler.save_data(df, 'btc_orderbook')
        
    elif choice == '6':
        logger.info("🔄 Crawling all sources...")
        crawler.scheduled_crawl_job()
        
    elif choice == '7':
        interval = input("Nhập interval (phút, mặc định 60): ").strip()
        interval = int(interval) if interval.isdigit() else 60
        print(f"\n⏰ Bắt đầu scheduled crawler (mỗi {interval} phút)...")
        print("Nhấn Ctrl+C để dừng")
        try:
            crawler.run_scheduler(interval_minutes=interval)
        except KeyboardInterrupt:
            logger.info("\n⏹️ Stopped scheduler")
    
    else:
        print("❌ Lựa chọn không hợp lệ!")
    
    print("\n✅ Hoàn thành!")


if __name__ == "__main__":
    main()
