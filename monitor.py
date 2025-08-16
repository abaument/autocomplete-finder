"""Monitor profit using in-memory price store instead of HTTP requests."""

import time
from price_stream import PriceStore


price_store = PriceStore()


def monitor_profit_and_sell(mint: str, entry_price: float, target_profit: float) -> None:
    """Check in-memory price and trigger a sell on target profit."""
    while True:
        current = price_store.get_price(mint)
        if current is None:
            time.sleep(1)
            continue
        profit = (current - entry_price) / entry_price
        if profit >= target_profit:
            print(f"Selling {mint} at {current} with profit {profit:.2%}")
            break
        time.sleep(1)
