from collections import defaultdict, deque
from typing import Dict


def process(input_queue, output_queue, window: int = 3):
    """Processor that reads coin price events from input_queue, computes SMA per coin,
    and forwards enriched events to output_queue.

    This function expects `None` as end-of-stream sentinel.
    """
    history = defaultdict(lambda: deque(maxlen=window))

    while True:
        item = input_queue.get()
        if item is None:
            # Propagate end-of-stream and exit
            output_queue.put(None)
            print("[Processor] End-of-stream received. Exiting.")
            break

        coin_id = item.get("id")
        price = item.get("price")
        if price is None or coin_id is None:
            continue

        history[coin_id].append(price)
        prices = list(history[coin_id])
        sma = sum(prices) / len(prices)

        enriched = dict(item)
        enriched["sma_{}".format(len(prices))] = round(sma, 8)
        enriched["window_size"] = len(prices)

        output_queue.put(enriched)
        print(f"[Processor] Processed {coin_id} price {price} -> sma({len(prices)})={sma:.8f}")
