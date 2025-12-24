import asyncio
import os
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

import orjson

from shared.logger import log

# Environment configuration
DRIFT_SAMPLE_FILE = os.getenv("DRIFT_SAMPLE_FILE", "data/drift_samples.jsonl")
INCIDENTS_DIR = os.getenv("INCIDENTS_DIR", "data/incidents")
DUPLICATE_LRU_MAX = int(os.getenv("DUPLICATE_LRU_MAX", "50000"))
LATENCY_BUFFER_SIZE = int(os.getenv("LATENCY_BUFFER_SIZE", "3000"))
LATENCY_SPIKE_THRESHOLD_MS = int(os.getenv("LATENCY_SPIKE_THRESHOLD_MS", "100"))
LATENCY_SPIKE_CONSECUTIVE = int(os.getenv("LATENCY_SPIKE_CONSECUTIVE", "2"))
FLIGHT_PRE_EVENTS = int(os.getenv("FLIGHT_PRE_EVENTS", "5000"))
FLIGHT_POST_EVENTS = int(os.getenv("FLIGHT_POST_EVENTS", "2000"))
FLIGHT_COOLDOWN_S = int(os.getenv("FLIGHT_COOLDOWN_S", "60"))

# Schema requirements
REQUIRED_KEYS = {"type", "product_id", "price", "last_size", "time", "ingest_ts_ms"}
EXPECTED_TYPES = {
    "type": str,
    "product_id": str,
    "price": (int, float),
    "last_size": (int, float),
    "time": str,
    "ingest_ts_ms": int,
}


class BusLike(Protocol):
    def subscribe(self, maxsize: int = 1000) -> asyncio.Queue: ...


@dataclass
class ForensicsCounters:
    processed: int = 0
    drift: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    gaps: int = 0
    spikes: int = 0
    incidents: int = 0


@dataclass
class DriftResult:
    is_drift: bool
    missing_keys: list[str] = field(default_factory=list)
    type_mismatches: dict[str, str] = field(default_factory=dict)
    unexpected_keys: list[str] = field(default_factory=list)


def check_schema_drift(event: dict) -> DriftResult:
    """Check if event has schema drift from expected format."""
    missing = [k for k in REQUIRED_KEYS if k not in event]
    type_mismatches = {}
    unexpected = [
        k
        for k in event.keys()
        if k not in REQUIRED_KEYS and k not in ("recv_ts_ms", "trade_id", "sequence")
    ]

    for key, expected_type in EXPECTED_TYPES.items():
        if key in event:
            val = event[key]
            if not isinstance(val, expected_type):
                type_mismatches[key] = (
                    f"expected {expected_type}, got {type(val).__name__}"
                )

    is_drift = bool(missing or type_mismatches)
    return DriftResult(
        is_drift=is_drift,
        missing_keys=missing,
        type_mismatches=type_mismatches,
        unexpected_keys=unexpected,
    )


class LRUSet:
    """LRU-evicting set for duplicate detection.
    Args:
        maxsize: The maximum size of the set.

    How it works:
        - When an item is added, it is added to the set.
        - If the set is full, the oldest item is removed.
        - If the item is already in the set, it is moved to the end of the set.
        - If the item is not in the set, it is added to the set.
    """

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._data: OrderedDict[Any, None] = OrderedDict()

    def __contains__(self, item: Any) -> bool:
        if item in self._data:
            self._data.move_to_end(item)
            return True
        return False

    def add(self, item: Any) -> None:
        if item in self._data:
            self._data.move_to_end(item)
        else:
            self._data[item] = None
            if len(self._data) > self.maxsize:
                self._data.popitem(last=False)


@dataclass
class SymbolState:
    last_exchange_ts_ms: int = 0
    last_sequence: int | None = None
    trade_ids: LRUSet = field(default_factory=lambda: LRUSet(DUPLICATE_LRU_MAX))


class IntegrityTracker:
    """Track integrity per product_id.

    How it works:
        - When an event is received, the product ID is extracted.
        - If the product ID is not in the state, a new state is created.
        - If the product ID is in the state, the event is checked for duplicates, out of order, and gaps.
        - The trade ID is added to the LRU set.
        - The last exchange timestamp is updated.
        - The last sequence is updated.
    """

    def __init__(self) -> None:
        self._states: dict[str, SymbolState] = {}

    def _get_state(self, product_id: str) -> SymbolState:
        if product_id not in self._states:
            self._states[product_id] = SymbolState()
        return self._states[product_id]

    def check(self, event: dict) -> tuple[bool, bool, bool]:
        """
        Returns (is_duplicate, is_out_of_order, is_gap).
        """
        product_id = event.get("product_id", "unknown")
        state = self._get_state(product_id)

        is_duplicate = False
        is_out_of_order = False
        is_gap = False

        trade_id = event.get("trade_id")
        if trade_id is not None:
            if trade_id in state.trade_ids:
                is_duplicate = True
            else:
                state.trade_ids.add(trade_id)

        time_str = event.get("time", "")
        if time_str:
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                exchange_ts_ms = int(dt.timestamp() * 1000)
                if (
                    state.last_exchange_ts_ms > 0
                    and exchange_ts_ms < state.last_exchange_ts_ms
                ):
                    is_out_of_order = True
                state.last_exchange_ts_ms = max(
                    state.last_exchange_ts_ms, exchange_ts_ms
                )
            except (ValueError, AttributeError):
                pass

        sequence = event.get("sequence")
        if sequence is not None and state.last_sequence is not None:
            if sequence > state.last_sequence + 1:
                is_gap = True
        if sequence is not None:
            state.last_sequence = sequence

        return is_duplicate, is_out_of_order, is_gap


class LatencySpikeDetector:
    """Detect latency spikes using rolling p99."""

    def __init__(
        self,
        buffer_size: int = LATENCY_BUFFER_SIZE,
        threshold_ms: int = LATENCY_SPIKE_THRESHOLD_MS,
        consecutive_required: int = LATENCY_SPIKE_CONSECUTIVE,
    ) -> None:
        self.buffer_size = buffer_size
        self.threshold_ms = threshold_ms
        self.consecutive_required = consecutive_required
        self._latencies: deque[int] = deque(maxlen=buffer_size)
        self._consecutive_spikes = 0

    def add_sample(self, ingest_ts_ms: int, recv_ts_ms: int) -> bool:
        """
        Add latency sample and return True if spike detected.
        Latency = recv_ts_ms - ingest_ts_ms (P2 receive time - P1 ingest time).
        """
        latency = recv_ts_ms - ingest_ts_ms
        if latency < 0:
            latency = 0
        self._latencies.append(latency)

        if len(self._latencies) < 100:
            return False

        sorted_latencies = sorted(self._latencies)
        p99_idx = int(len(sorted_latencies) * 0.99)
        p99 = sorted_latencies[p99_idx]

        if p99 > self.threshold_ms:
            self._consecutive_spikes += 1
            if self._consecutive_spikes >= self.consecutive_required:
                self._consecutive_spikes = 0
                return True
        else:
            self._consecutive_spikes = 0

        return False

    def get_p99(self) -> int:
        """Get current p99 latency."""
        if len(self._latencies) < 10:
            return 0
        sorted_latencies = sorted(self._latencies)
        p99_idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[p99_idx]


class DriftSampleWriter:
    """Async writer for drift samples."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self._q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1000)
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="drift_writer")

    def write(self, event: dict, drift_result: DriftResult) -> None:
        sample = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "missing_keys": drift_result.missing_keys,
            "type_mismatches": drift_result.type_mismatches,
            "unexpected_keys": drift_result.unexpected_keys,
        }
        try:
            self._q.put_nowait(orjson.dumps(sample) + b"\n")
        except asyncio.QueueFull:
            pass

    async def _run(self) -> None:
        f = open(self.file_path, "ab", buffering=65536)
        try:
            while True:
                line = await self._q.get()
                await asyncio.to_thread(f.write, line)
                await asyncio.to_thread(f.flush)
        finally:
            try:
                f.close()
            except Exception:
                pass


class FlightRecorder:
    """Ring buffer flight recorder for incident capture."""

    def __init__(
        self,
        incidents_dir: str = INCIDENTS_DIR,
        pre_events: int = FLIGHT_PRE_EVENTS,
        post_events: int = FLIGHT_POST_EVENTS,
        cooldown_s: int = FLIGHT_COOLDOWN_S,
    ) -> None:
        self.incidents_dir = incidents_dir
        self.pre_events = pre_events
        self.post_events = post_events
        self.cooldown_s = cooldown_s
        self._ring: deque[dict] = deque(maxlen=pre_events)
        self._capturing = False
        self._capture_buffer: list[dict] = []
        self._capture_remaining = 0
        self._last_incident_time = 0.0
        self._incident_reason = ""
        self._incident_count = 0

    def record(self, event: dict) -> None:
        """Record event to ring buffer or capture buffer."""
        if self._capturing:
            self._capture_buffer.append(event)
            self._capture_remaining -= 1
            if self._capture_remaining <= 0:
                self._finalize_incident()
        else:
            self._ring.append(event)

    def trigger(self, reason: str) -> bool:
        """
        Trigger incident capture. Returns True if triggered, False if on cooldown.
        """
        now = time.monotonic()
        if self._capturing:
            return False
        if (now - self._last_incident_time) < self.cooldown_s:
            return False

        self._capturing = True
        self._capture_buffer = list(self._ring)
        self._capture_remaining = self.post_events
        self._incident_reason = reason
        self._last_incident_time = now
        log.warning(f"Incident triggered: {reason}")
        return True

    def _finalize_incident(self) -> None:
        """Save incident to disk."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        incident_id = f"{ts}_{uuid.uuid4().hex[:8]}"
        incident_dir = os.path.join(self.incidents_dir, incident_id)

        try:
            os.makedirs(incident_dir, exist_ok=True)

            events_path = os.path.join(incident_dir, "events.jsonl")
            with open(events_path, "wb") as f:
                for event in self._capture_buffer:
                    f.write(orjson.dumps(event) + b"\n")

            meta = {
                "incident_id": incident_id,
                "reason": self._incident_reason,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "pre_events": len([e for e in self._capture_buffer[: self.pre_events]]),
                "post_events": len(self._capture_buffer) - self.pre_events,
                "total_events": len(self._capture_buffer),
            }
            meta_path = os.path.join(incident_dir, "meta.json")
            with open(meta_path, "wb") as f:
                f.write(orjson.dumps(meta, option=orjson.OPT_INDENT_2))

            log.info(
                f"Incident saved: {incident_dir} ({len(self._capture_buffer)} events)"
            )
            self._incident_count += 1

        except Exception as e:
            log.error(f"Failed to save incident: {e}")

        self._capturing = False
        self._capture_buffer = []
        self._ring.clear()

    @property
    def incidents_captured(self) -> int:
        return self._incident_count


async def consumer_forensics(bus: BusLike, print_every_s: float = 10.0) -> None:
    """Forensics consumer for schema drift, integrity, and latency spike detection."""
    q = bus.subscribe(maxsize=5000)
    counters = ForensicsCounters()

    integrity_tracker = IntegrityTracker()
    latency_detector = LatencySpikeDetector()
    drift_writer = DriftSampleWriter(DRIFT_SAMPLE_FILE)
    flight_recorder = FlightRecorder()

    await drift_writer.start()

    last_print = time.monotonic()

    while True:
        event = await q.get()
        counters.processed += 1

        flight_recorder.record(event)

        drift_result = check_schema_drift(event)
        if drift_result.is_drift:
            counters.drift += 1
            drift_writer.write(event, drift_result)

        is_dup, is_ooo, is_gap = integrity_tracker.check(event)
        if is_dup:
            counters.duplicates += 1
        if is_ooo:
            counters.out_of_order += 1
        if is_gap:
            counters.gaps += 1

        ingest_ts = event.get("ingest_ts_ms", 0)
        recv_ts = event.get("recv_ts_ms", 0)
        if ingest_ts and recv_ts:
            is_spike = latency_detector.add_sample(ingest_ts, recv_ts)
            if is_spike:
                counters.spikes += 1
                triggered = flight_recorder.trigger(
                    f"latency_spike_p99={latency_detector.get_p99()}ms"
                )
                if triggered:
                    counters.incidents = flight_recorder.incidents_captured

        if is_dup:
            flight_recorder.trigger("duplicate_detected")
            counters.incidents = flight_recorder.incidents_captured

        if is_gap:
            flight_recorder.trigger(f"sequence_gap")
            counters.incidents = flight_recorder.incidents_captured

        now = time.monotonic()
        if (now - last_print) >= print_every_s:
            log.info(
                f"FORENSICS | processed={counters.processed} | drift={counters.drift} | "
                f"dup={counters.duplicates} | ooo={counters.out_of_order} | gaps={counters.gaps} | "
                f"spikes={counters.spikes} | incidents={counters.incidents}"
            )
            last_print = now
