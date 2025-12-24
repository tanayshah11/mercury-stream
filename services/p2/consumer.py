import asyncio
import time
from collections import deque
from typing import Protocol

from shared.logger import log


def now_ms() -> int:
    return int(time.time() * 1000)


def percentile(sorted_vals: list[int], p: float) -> float:
    """Calculate the percentile of a list of values.
    Args:
        sorted_vals: The list of values to calculate the percentile of.
        p: The percentile to calculate.
    Returns:
        The percentile of the list of values.
    """
    if not sorted_vals:
        return 0.0
    k = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    k = max(0, min(k, len(sorted_vals) - 1))
    return float(sorted_vals[k])


class BusLike(Protocol):
    """A protocol for the bus.
    Args:
        drops: The number of drops.
        subs: The list of subscribers.
    """

    drops: int
    subs: list[asyncio.Queue]

    def subscribe(self, maxsize: int = 1000) -> asyncio.Queue: ...

    """Subscribe to the bus.
    Args:
        maxsize: The maximum size of the queue.
    Returns:
        The queue.
    """

    def queue_depths(self) -> list[int]: ...

    """Get the queue depths.
    Returns:
        The queue depths.
    """


async def consumer_vwap(bus: BusLike, window_n: int = 200, print_every_s: float = 1.0):
    """Consume the bus and print the VWAP.
    Args:
        bus: The bus to consume.
        window_n: The size of the window.
        print_every_s: The time to print the VWAP.

    How it works:
        - The bus is subscribed to.
        - The window is a deque of the last window_n events.
        - The ages_ms is a deque of the last 3000 events.
        - The pipes_ms is a deque of the last 3000 events.
        - The last_print is the time of the last print.
        - The print_every_s is the time to print the VWAP.
        - The VWAP is calculated as the sum of the prices * sizes / the sum of the sizes.
        - The ages_sorted is the sorted ages_ms.

    """
    q = bus.subscribe(maxsize=1000)

    window = deque(maxlen=window_n)
    # ages_ms represents the end-to-end latency of the event from the exchange to the consumer.
    ages_ms: deque[int] = deque(maxlen=3000)
    # pipes_ms represents the latency of the event from P1 to P2.
    pipes_ms: deque[int] = deque(maxlen=3000)
    # last_print is the time of the last print.
    last_print = time.time()

    while True:
        try:
            e = await q.get()
            if not isinstance(e, dict):
                continue

            try:
                # Extract the price, size, ingest timestamp, and receive timestamp.
                price = float(e.get("price", 0.0))
                size = float(e.get("last_size", 0.0))
                ingest_ts = int(e.get("ingest_ts_ms", 0))
                recv_ts = e.get("recv_ts_ms")
                recv_ts = int(recv_ts) if recv_ts is not None else 0
                # If the receive timestamp is not present, set it to 0.
            except Exception:
                continue

            # If the price is not present continue forward to the next event
            if price <= 0 or size < 0 or ingest_ts <= 0:
                continue

            # Add the price and size to the window.
            window.append((price, size))
            # Calculate the end-to-end latency of the event.
            now = now_ms()
            ages_ms.append(max(0, now - ingest_ts))
            if recv_ts > 0:
                # Calculate the latency of the event from P1 to P2.
                pipes_ms.append(max(0, now - recv_ts))

            if (time.time() - last_print) >= print_every_s:
                # If the window is empty, set the last print time to the current time and continue forward to the next event.
                if not window:
                    last_print = time.time()
                    continue

                # Calculate the VWAP.
                num = sum(p * s for p, s in window)
                den = sum(s for _, s in window)
                vwap = (num / den) if den > 0 else 0.0

                # Calculate the percentiles of the ages_ms.
                ages_sorted = sorted(ages_ms)
                age_p50 = percentile(ages_sorted, 50)
                age_p95 = percentile(ages_sorted, 95)
                age_p99 = percentile(ages_sorted, 99)

                # Calculate the percentiles of the pipes_ms.
                pipes_sorted = sorted(pipes_ms)
                pipe_p50 = percentile(pipes_sorted, 50)
                pipe_p95 = percentile(pipes_sorted, 95)
                pipe_p99 = percentile(pipes_sorted, 99)

                # Print the VWAP, ages, and pipes.
                log.info(
                    f"VWAP={vwap:.2f} | age p50={age_p50:.0f} p95={age_p95:.0f} p99={age_p99:.0f} | "
                    f"pipe p50={pipe_p50:.0f} p95={pipe_p95:.0f} p99={pipe_p99:.0f} | drops={bus.drops}"
                )
                # Set the last print time to the current time.
                last_print = time.time()

        except Exception as ex:
            log.error(f"Error in consumer_vwap: {ex}")


async def consumer_health(bus: BusLike, print_every_s: float = 5.0):
    """Consume the bus and print the health.
    Args:
        bus: The bus to consume.
        print_every_s: The time to print the health.

    How it works:
        - The bus is subscribed to.
        - The last_print is the time of the last print.
        - The count is the number of events processed.
        - The last_price is the last price.
    """
    q = bus.subscribe(maxsize=1000)
    # last_print is the time of the last print.
    last_print = time.time()
    # count is the number of events processed.
    count = 0
    last_price = None

    while True:
        try:
            e = await q.get()
            # Increment the count.
            count += 1
            # Try to extract the price from the event.
            try:
                if isinstance(e, dict) and e.get("price") is not None:
                    last_price = float(e.get("price", last_price))
            except Exception:
                pass

            now = time.time()
            # If the time since the last print is greater than the print_every_s, print the health.
            if (now - last_print) >= print_every_s:
                dt = now - last_print
                # Calculate the events per second.
                eps = (count / dt) if dt > 0 else 0.0
                # Get the queue depths.
                depths = bus.queue_depths()

                log.info(
                    f"HEALTH | eps={eps:.1f} | price={last_price} | "
                    f"drops={bus.drops} | subs={len(bus.subs)} | qdepths={depths}"
                )
                count = 0
                last_print = now

        except Exception as ex:
            log.error(f"Error in consumer_health: {ex}")


async def consumer_slow(bus: BusLike, sleep_ms: int = 50):
    """Consume the bus and sleep for the sleep_ms.
    Args:
        bus: The bus to consume.
        sleep_ms: The time to sleep.

    How it works:
        - The bus is subscribed to.
        - The sleep_ms is the time to sleep.
        - The q is the queue.
        - The _ is the event. It is ignored.
        - The await asyncio.sleep(sleep_ms / 1000.0) is the time to sleep.
    """
    q = bus.subscribe(maxsize=200)

    while True:
        try:
            _ = await q.get()  # consume the event although we don't use it
            # Sleep for the sleep_ms.
            await asyncio.sleep(sleep_ms / 1000.0)
        except Exception as ex:
            log.error(f"Error in consumer_slow: {ex}")
