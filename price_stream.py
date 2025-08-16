"""WebSocket price client using Pyth network."""

import asyncio
import json
from typing import Dict

import websockets


class PriceStore:
    """In-memory store for latest prices keyed by mint/ID."""

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}

    def update_price(self, mint: str, price: float) -> None:
        self._prices[mint] = price

    def get_price(self, mint: str) -> float | None:
        return self._prices.get(mint)


class PythPriceClient:
    """Subscribe to Pyth price feed and update the store."""

    def __init__(self, price_id: str, store: PriceStore) -> None:
        self.price_id = price_id
        self.store = store
        self.url = "wss://hermes.pyth.network/ws"

    async def run(self) -> None:
        async with websockets.connect(self.url) as ws:
            subscribe_msg = {"type": "subscribe", "ids": [self.price_id]}
            await ws.send(json.dumps(subscribe_msg))
            async for raw in ws:
                data = json.loads(raw)
                price = data.get("price")
                if price is not None:
                    self.store.update_price(self.price_id, price)
