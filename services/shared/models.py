from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class Ticker(BaseModel):
    """Complete Coinbase ticker event with all available market data.

    Core trade data:
        type, product_id, price, last_size, side, trade_id, time, sequence

    24-hour stats:
        open_24h, high_24h, low_24h, volume_24h

    30-day stats:
        volume_30d

    Order book top-of-book:
        best_bid, best_bid_size, best_ask, best_ask_size
    """

    # Message metadata
    type: Literal["ticker"]
    sequence: int  # For OOO detection - monotonically increasing per product
    product_id: str  # Trading pair (e.g., "BTC-USD")

    # Trade data
    price: float  # Last trade price
    last_size: float = 0.0  # Last trade size
    side: Literal["buy", "sell"] | None = None  # Trade side (taker direction)
    trade_id: int | None = None  # Unique trade identifier

    # Timestamp
    time: str  # ISO 8601 timestamp from exchange

    # 24-hour rolling window stats
    open_24h: float | None = None  # Opening price 24h ago
    high_24h: float | None = None  # Highest price in 24h
    low_24h: float | None = None  # Lowest price in 24h
    volume_24h: float | None = None  # Total volume in 24h (base currency)

    # 30-day stats
    volume_30d: float | None = None  # Total volume in 30 days

    # Top-of-book (best bid/ask)
    best_bid: float | None = None  # Highest buy order price
    best_bid_size: float | None = None  # Size at best bid
    best_ask: float | None = None  # Lowest sell order price
    best_ask_size: float | None = None  # Size at best ask

    @property
    def exchange_ts_ms(self) -> int:
        """Convert ISO 8601 time to milliseconds since epoch."""
        dt = datetime.fromisoformat(self.time.replace("Z", "+00:00"))
        # Ensure timezone-aware datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    @property
    def spread(self) -> float | None:
        """Bid-ask spread in quote currency (e.g., USD).

        Returns None if spread is negative (inverted book - bad data).
        """
        if self.best_ask is not None and self.best_bid is not None:
            spread_value = self.best_ask - self.best_bid
            # Return None for inverted spreads (bad data)
            return spread_value if spread_value >= 0 else None
        return None

    @property
    def spread_bps(self) -> float | None:
        """Bid-ask spread in basis points (1 bp = 0.01%)."""
        if self.spread is not None and self.best_bid:
            return (self.spread / self.best_bid) * 10000
        return None

    @property
    def mid_price(self) -> float | None:
        """Mid-market price (average of bid and ask)."""
        if self.best_ask is not None and self.best_bid is not None:
            return (self.best_ask + self.best_bid) / 2
        return None

    @property
    def range_24h(self) -> float | None:
        """24-hour price range (high - low)."""
        if self.high_24h is not None and self.low_24h is not None:
            return self.high_24h - self.low_24h
        return None

    @property
    def change_24h(self) -> float | None:
        """24-hour price change from open."""
        if self.open_24h is not None and self.open_24h > 0:
            return self.price - self.open_24h
        return None

    @property
    def change_24h_pct(self) -> float | None:
        """24-hour price change as percentage."""
        if self.open_24h is not None and self.open_24h > 0:
            return ((self.price - self.open_24h) / self.open_24h) * 100
        return None
