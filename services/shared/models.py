from datetime import datetime

from pydantic import BaseModel


class Ticker(BaseModel):
    """Pydantic model for Coinbase tickers.
    Args:
        type: The type of the event.
        product_id: The product ID.
        price: The price of the event.
        last_size: The last size of the event.
        time: The time of the event.
        trade_id: The trade ID of the event.
    """

    type: str
    product_id: str
    price: float
    last_size: float = 0.0
    time: str
    trade_id: int | None = None

    @property
    def exchange_ts_ms(self) -> int:
        """Convert ISO 8601 time to milliseconds since epoch."""
        dt = datetime.fromisoformat(self.time.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
