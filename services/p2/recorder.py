import asyncio
import os
import time
from typing import Optional

import orjson

from shared.logger import log


class Recorder:
    """Recorder for the events.
    Args:
        file_path: The path to the file to record the events to.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=10_000)
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the recorder.
        Args:
            file_path: The path to the file to record the events to.
        How it works:
            - The file is created if it doesn't exist.
            - The task is created to run the recorder.
            - The recorder is started.
        """
        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="recorder")
            log.debug(f"Recorder started: {self.file_path}")

    def record(self, event: dict) -> None:
        """Record the event.
        Args:
            event: The event to record.

        How it works:
            - The event is serialized to a JSON string.
            - The JSON string is added to the queue.
            - If the queue is full, the event is dropped.
        """
        try:
            # Serialize the event to a JSON string.
            line = orjson.dumps(event) + b"\n"
            self._q.put_nowait(line)
        except asyncio.QueueFull:
            log.warning("Recorder queue full, dropping event")
        except Exception:
            pass

    async def _run(self) -> None:
        """Run the recorder.
        Args:
            file_path: The path to the file to record the events to.
        How it works:
            - The file is opened in append mode.
            - The last_flush is the time of the last flush.
            - The pending is the number of events pending to be written.
        """
        # Open the file in append mode with a 1MB buffer.
        f = open(self.file_path, "ab", buffering=1024 * 1024)
        # last_flush is the time of the last flush.
        last_flush = time.monotonic()  # monotonic is not affected by the system time.
        # pending is the number of events pending to be written.
        pending = 0
        try:
            while True:
                # Get the next event from the queue.
                line = await self._q.get()
                # Write the event to the file in a background non-blocking way.
                await asyncio.to_thread(f.write, line)
                pending += 1
                # If the pending is greater than 200 or the time since the last flush is greater than 1 second, flush the file.
                now = time.monotonic()
                if pending >= 200 or (now - last_flush) >= 1.0:
                    try:
                        # Flush the file in a background non-blocking way.
                        await asyncio.to_thread(f.flush)
                    except Exception:
                        pass
                    pending = 0
                    last_flush = now
        finally:
            try:
                # Flush the file in a background non-blocking way.
                await asyncio.to_thread(f.flush)
            except Exception:
                pass
            try:
                # Close the file.
                f.close()
            except Exception:
                pass
