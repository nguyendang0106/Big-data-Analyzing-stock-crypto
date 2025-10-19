"""
Cryptocurrency Data Crawler
Crawl dữ liệu tiền ảo từ CoinGecko API và lưu vào CSV/JSON
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime
import os


class CryptoCrawler:
    """Class để crawl dữ liệu tiền ảo từ CoinGecko API"""
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
    
    def get_top_cryptocurrencies(self, limit=100, vs_currency='usd'):
        """
        Lấy danh sách top cryptocurrencies
        
        Args:
            limit: Số lượng coin muốn lấy (mặc định 100)
            vs_currency: Loại tiền tệ so sánh (mặc định USD)
        
        Returns:
            DataFrame chứa thông tin các cryptocurrency
        """
        print(f"🔄 Đang crawl top {limit} cryptocurrencies...")
        
        url = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': limit,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h,24h,7d,30d'
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            df = pd.DataFrame(data)
            df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"✅ Đã crawl thành công {len(df)} cryptocurrencies")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi crawl dữ liệu: {e}")
            return None
    
    def get_coin_details(self, coin_id):
        """
        Lấy thông tin chi tiết về một cryptocurrency
        
        Args:
            coin_id: ID của coin (ví dụ: 'bitcoin', 'ethereum')
        
        Returns:
            Dictionary chứa thông tin chi tiết
        """
        print(f"🔄 Đang crawl thông tin chi tiết cho {coin_id}...")
        
        url = f"{self.base_url}/coins/{coin_id}"
        params = {
            'localization': False,
            'tickers': False,
            'market_data': True,
            'community_data': True,
            'developer_data': True
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            print(f"✅ Đã crawl thành công thông tin {coin_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi crawl dữ liệu: {e}")
            return None
    
    def get_historical_data(self, coin_id, days=30, vs_currency='usd'):
        """
        Lấy dữ liệu lịch sử giá của cryptocurrency
        
        Args:
            coin_id: ID của coin
            days: Số ngày lịch sử (max 365 cho free API)
            vs_currency: Loại tiền tệ
        
        Returns:
            DataFrame chứa dữ liệu lịch sử
        """
        print(f"🔄 Đang crawl dữ liệu lịch sử {days} ngày cho {coin_id}...")
        
        url = f"{self.base_url}/coins/{coin_id}/market_chart"
        params = {
            'vs_currency': vs_currency,
            'days': days,
            'interval': 'daily' if days > 1 else 'hourly'
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            # Chuyển đổi sang DataFrame
            prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            volumes = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
            market_caps = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
            
            # Merge các DataFrame
            df = prices.merge(volumes, on='timestamp').merge(market_caps, on='timestamp')
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['coin_id'] = coin_id
            
            print(f"✅ Đã crawl thành công {len(df)} records lịch sử")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi crawl dữ liệu: {e}")
            return None
    
    def get_trending_coins(self):
        """
        Lấy danh sách các coin đang trending
        
        Returns:
            List các coin đang trending
        """
        print("🔄 Đang crawl trending coins...")
        
        url = f"{self.base_url}/search/trending"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            trending = data['coins']
            df = pd.DataFrame([coin['item'] for coin in trending])
            df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"✅ Đã crawl thành công {len(df)} trending coins")
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi crawl dữ liệu: {e}")
            return None
    
    def get_global_data(self):
        """
        Lấy dữ liệu thị trường crypto toàn cầu
        
        Returns:
            Dictionary chứa dữ liệu global
        """
        print("🔄 Đang crawl global market data...")
        
        url = f"{self.base_url}/global"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            print("✅ Đã crawl thành công global data")
            return data['data']
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi crawl dữ liệu: {e}")
            return None
    
    def save_to_csv(self, df, filename):
        """Lưu DataFrame vào file CSV"""
        if df is not None and not df.empty:
            filepath = f"crypto_data_{filename}.csv"
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"💾 Đã lưu dữ liệu vào {filepath}")
            return filepath
        return None
    
    def save_to_json(self, data, filename):
        """Lưu dữ liệu vào file JSON"""
        if data is not None:
            filepath = f"crypto_data_{filename}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"💾 Đã lưu dữ liệu vào {filepath}")
            return filepath
        return None


def main():
    """Hàm chính để chạy crawler"""
    
    print("=" * 60)
    print("🚀 CRYPTOCURRENCY DATA CRAWLER")
    print("=" * 60)
    
    # Khởi tạo crawler
    crawler = CryptoCrawler()
    
    # Tạo thư mục lưu trữ nếu chưa có
    os.makedirs('crypto_data', exist_ok=True)
    os.chdir('crypto_data')
    
    # 1. Crawl top 50 cryptocurrencies
    print("\n📊 Task 1: Crawl Top 50 Cryptocurrencies")
    print("-" * 60)
    top_coins = crawler.get_top_cryptocurrencies(limit=50)
    if top_coins is not None:
        crawler.save_to_csv(top_coins, f"top50_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        print(f"\nTop 5 Cryptocurrencies:")
        print(top_coins[['name', 'symbol', 'current_price', 'market_cap', 'price_change_percentage_24h']].head())
    
    time.sleep(2)  # Delay để tránh rate limit
    
    # 2. Crawl trending coins
    print("\n\n🔥 Task 2: Crawl Trending Coins")
    print("-" * 60)
    trending = crawler.get_trending_coins()
    if trending is not None:
        crawler.save_to_csv(trending, f"trending_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        print(f"\nTrending Coins:")
        print(trending[['name', 'symbol', 'market_cap_rank']].head(10))
    
    time.sleep(2)
    
    # 3. Crawl dữ liệu lịch sử Bitcoin và Ethereum
    print("\n\n📈 Task 3: Crawl Historical Data")
    print("-" * 60)
    
    for coin in ['bitcoin', 'ethereum']:
        historical = crawler.get_historical_data(coin, days=30)
        if historical is not None:
            crawler.save_to_csv(historical, f"{coin}_30days_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            print(f"\n{coin.upper()} - Last 5 records:")
            print(historical[['timestamp', 'price', 'volume', 'market_cap']].tail())
        time.sleep(2)
    
    # 4. Crawl global market data
    print("\n\n🌍 Task 4: Crawl Global Market Data")
    print("-" * 60)
    global_data = crawler.get_global_data()
    if global_data is not None:
        crawler.save_to_json(global_data, f"global_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        print(f"\nGlobal Market Data:")
        print(f"Total Market Cap: ${global_data.get('total_market_cap', {}).get('usd', 0):,.0f}")
        print(f"Total Volume (24h): ${global_data.get('total_volume', {}).get('usd', 0):,.0f}")
        print(f"Active Cryptocurrencies: {global_data.get('active_cryptocurrencies', 0):,}")
        print(f"Bitcoin Dominance: {global_data.get('market_cap_percentage', {}).get('btc', 0):.2f}%")
    
    # 5. Crawl thông tin chi tiết Bitcoin
    print("\n\n🔍 Task 5: Crawl Bitcoin Detailed Info")
    print("-" * 60)
    btc_details = crawler.get_coin_details('bitcoin')
    if btc_details is not None:
        crawler.save_to_json(btc_details, f"bitcoin_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        print(f"\nBitcoin Details:")
        print(f"Name: {btc_details.get('name')}")
        print(f"Symbol: {btc_details.get('symbol', '').upper()}")
        print(f"Hashing Algorithm: {btc_details.get('hashing_algorithm')}")
        print(f"All Time High: ${btc_details.get('market_data', {}).get('ath', {}).get('usd', 0):,.0f}")
    
    print("\n" + "=" * 60)
    print("✅ HOÀN THÀNH CRAWL DỮ LIỆU!")
    print("=" * 60)
    print(f"📁 Dữ liệu đã được lưu trong thư mục: {os.getcwd()}")


if __name__ == "__main__":
    # Chạy crawler
    main()
    
    print("\n💡 Tips:")
    print("- Có thể thay đổi limit, days, coin_id theo nhu cầu")
    print("- API CoinGecko free có giới hạn ~10-50 requests/phút")
    print("- Sử dụng time.sleep() để tránh bị rate limit")
    print("- Có thể lên lịch chạy định kỳ bằng cron hoặc schedule")
