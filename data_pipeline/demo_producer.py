import time
import requests
from typing import List, Dict

COINGECKO_MARKETS_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
)


def fetch_top_coins(vs_currency: str = "usd", per_page: int = 10) -> List[Dict]:
    """Fetch top coins market data from CoinGecko."""
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": "false",
    }
    resp = requests.get(COINGECKO_MARKETS_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def produce(queue, interval: float = 5.0, iterations: int = 3):
    """Producer that fetches top coins every `interval` seconds and puts them to queue.

    Args:
        queue: queue.Queue-like object with `put` method
        interval: seconds between fetches
        iterations: how many fetch cycles to run (for demo)
    """
    for i in range(iterations):
        try:
            data = fetch_top_coins(per_page=10)
            timestamp = int(time.time())
            for coin in data:
                msg = {
                    "timestamp": timestamp,
                    "id": coin.get("id"),
                    "symbol": coin.get("symbol"),
                    "name": coin.get("name"),
                    "price": coin.get("current_price"),
                    "market_cap": coin.get("market_cap"),
                }
                queue.put(msg)
            print(f"[Producer] Published {len(data)} coins (cycle {i+1}/{iterations})")
        except Exception as e:
            print(f"[Producer] Error: {e}")
        time.sleep(interval)

    # Signal end-of-stream
    queue.put(None)
    print("[Producer] Done producing.")
