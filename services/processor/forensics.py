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

# Optional metrics integration - graceful degradation if metrics module unavailable
try:
    from metrics import record_event, record_anomaly, record_incident
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False
    def record_event(latency_ms=None): pass
    def record_anomaly(t): pass
    def record_incident(): pass

# Environment configuration
DRIFT_SAMPLE_FILE = os.getenv("DRIFT_SAMPLE_FILE", "data/drift_samples.jsonl")
INCIDENTS_DIR = os.getenv("INCIDENTS_DIR", "data/incidents")
DUPLICATE_LRU_MAX = int(os.getenv("DUPLICATE_LRU_MAX", "50000"))  # Max trade IDs tracked per symbol
LATENCY_BUFFER_SIZE = int(os.getenv("LATENCY_BUFFER_SIZE", "3000"))  # Rolling window for p99 calculation
LATENCY_SPIKE_THRESHOLD_MS = int(os.getenv("LATENCY_SPIKE_THRESHOLD_MS", "100"))  # p99 threshold to trigger spike
LATENCY_SPIKE_CONSECUTIVE = int(os.getenv("LATENCY_SPIKE_CONSECUTIVE", "2"))  # Consecutive spikes needed to trigger incident
# Fixed incident capture settings (not configurable)
FLIGHT_WINDOW_SECONDS = 600  # Rolling window: keep last 10 minutes of data
FLIGHT_PRE_EVENTS = 5000  # Max events to capture before incident trigger
FLIGHT_POST_EVENTS = 3000  # Events to capture after incident trigger
FLIGHT_COOLDOWN_S = int(os.getenv("FLIGHT_COOLDOWN_S", "60"))  # Prevent incident spam

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
    """Validate event schema against expected structure - detects missing/wrong types/extra fields."""
    missing = [k for k in REQUIRED_KEYS if k not in event]
    type_mismatches = {}
    # Allow optional fields: recv_ts_ms, trade_id, sequence, feeder_id
    unexpected = [
        k
        for k in event.keys()
        if k not in REQUIRED_KEYS and k not in ("recv_ts_ms", "trade_id", "sequence", "feeder_id")
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
    """Memory-efficient duplicate detection - keeps only recent trade IDs, evicts oldest."""

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._data: OrderedDict[Any, None] = OrderedDict()  # OrderedDict = O(1) lookup + LRU ordering

    def __contains__(self, item: Any) -> bool:
        if item in self._data:
            self._data.move_to_end(item)  # Touch to mark as recently used
            return True
        return False

    def add(self, item: Any) -> None:
        if item in self._data:
            self._data.move_to_end(item)  # Refresh position if already exists
        else:
            self._data[item] = None
            if len(self._data) > self.maxsize:
                self._data.popitem(last=False)  # Evict oldest (FIFO from front)


@dataclass
class SymbolState:
    """Per-symbol-per-feeder tracking state for integrity checks."""
    last_exchange_ts_ms: int = 0  # For out-of-order detection
    last_trade_id: int | None = None  # For gap detection (trade_ids are sequential per symbol)
    trade_ids: LRUSet = field(default_factory=lambda: LRUSet(DUPLICATE_LRU_MAX))  # For duplicate detection


class IntegrityTracker:
    """Per-symbol-per-feeder data quality monitoring - detects duplicates, reordering, sequence gaps.

    Tracks state per (product_id, feeder_id) to avoid false positives when multiple
    feeders send interleaved sequences for the same symbol.
    """

    def __init__(self, max_states: int = 100) -> None:
        self._states: OrderedDict[tuple[str, str], SymbolState] = OrderedDict()  # State per (product_id, feeder_id)
        self._max_states = max_states  # Prevent unbounded growth

    def _get_state(self, product_id: str, feeder_id: str) -> SymbolState:
        key = (product_id, feeder_id)
        if key not in self._states:
            self._states[key] = SymbolState()
            # LRU eviction: remove oldest state if exceeding max
            if len(self._states) > self._max_states:
                self._states.popitem(last=False)
        else:
            # Move to end to mark as recently used
            self._states.move_to_end(key)
        return self._states[key]

    def check(self, event: dict) -> tuple[bool, bool, bool]:
        """Returns (is_duplicate, is_out_of_order, is_gap)."""
        product_id = event.get("product_id", "unknown")
        feeder_id = event.get("feeder_id", "unknown")  # Track per feeder to avoid false positives
        state = self._get_state(product_id, feeder_id)

        is_duplicate = False
        is_out_of_order = False
        is_gap = False

        # Duplicate check: have we seen this trade_id before?
        trade_id = event.get("trade_id")
        if trade_id is not None:
            if trade_id in state.trade_ids:
                is_duplicate = True
            else:
                state.trade_ids.add(trade_id)

        # Out-of-order check: is exchange timestamp going backwards?
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

        # Gap check: is trade_id jumping by >1? (trade_ids are sequential per symbol)
        if trade_id is not None and state.last_trade_id is not None:
            if trade_id > state.last_trade_id + 1:
                is_gap = True
        if trade_id is not None:
            state.last_trade_id = trade_id

        return is_duplicate, is_out_of_order, is_gap


class LatencySpikeDetector:
    """Rolling p99 latency monitor - triggers on sustained high latency to avoid false positives."""

    def __init__(
        self,
        buffer_size: int = LATENCY_BUFFER_SIZE,
        threshold_ms: int = LATENCY_SPIKE_THRESHOLD_MS,
        consecutive_required: int = LATENCY_SPIKE_CONSECUTIVE,
    ) -> None:
        self.buffer_size = buffer_size
        self.threshold_ms = threshold_ms
        self.consecutive_required = consecutive_required
        self._latencies: deque[int] = deque(maxlen=buffer_size)  # Ring buffer auto-evicts oldest
        self._consecutive_spikes = 0  # Require N consecutive spikes to trigger

    def add_sample(self, ingest_ts_ms: int, recv_ts_ms: int) -> bool:
        """Returns True if sustained spike detected (latency = P2_recv - P1_ingest)."""
        latency = recv_ts_ms - ingest_ts_ms
        if latency < 0:
            latency = 0  # Clock skew protection
        self._latencies.append(latency)

        if len(self._latencies) < 100:  # Need baseline before detecting spikes
            return False

        # Calculate p99 from rolling window
        sorted_latencies = sorted(self._latencies)
        p99_idx = int(len(sorted_latencies) * 0.99)
        # Prevent off-by-one error when index equals length
        p99_idx = min(p99_idx, len(sorted_latencies) - 1)
        p99 = sorted_latencies[p99_idx]

        # Require consecutive spikes to avoid false positives from single outliers
        if p99 > self.threshold_ms:
            self._consecutive_spikes += 1
            if self._consecutive_spikes >= self.consecutive_required:
                self._consecutive_spikes = 0  # Reset counter after triggering
                return True
        else:
            self._consecutive_spikes = 0  # Reset on normal latency

        return False

    def get_p99(self) -> int:
        """Current p99 latency for incident metadata."""
        if len(self._latencies) < 10:
            return 0
        sorted_latencies = sorted(self._latencies)
        p99_idx = int(len(sorted_latencies) * 0.99)
        # Prevent off-by-one error when index equals length
        p99_idx = min(p99_idx, len(sorted_latencies) - 1)
        return sorted_latencies[p99_idx]


class DriftSampleWriter:
    """Background JSONL writer for schema drift samples - non-blocking, drops on backpressure."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self._q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1000)  # Bounded queue
        self._task: asyncio.Task | None = None
        self._shutdown = False

    async def start(self) -> None:
        os.makedirs(os.path.dirname(self.file_path) or ".", exist_ok=True)
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="drift_writer")

    async def shutdown(self) -> None:
        """Graceful shutdown: drain queue and close file."""
        self._shutdown = True
        if self._task:
            await self._task

    def write(self, event: dict, drift_result: DriftResult) -> None:
        """Non-blocking write - drops sample if queue full (don't block forensics)."""
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
            pass  # Drop sample on backpressure - don't block consumer

    async def _run(self) -> None:
        """Background task: drain queue to disk."""
        f = open(self.file_path, "ab", buffering=65536)
        try:
            while not self._shutdown or not self._q.empty():
                try:
                    line = await asyncio.wait_for(self._q.get(), timeout=0.5)
                    await asyncio.to_thread(f.write, line)  # Offload I/O to thread pool
                    await asyncio.to_thread(f.flush)
                except asyncio.TimeoutError:
                    continue  # Check shutdown flag
        finally:
            try:
                f.close()
            except Exception:
                pass


class FlightRecorder:
    """Black box recorder: 10-minute rolling window + post-incident capture."""

    def __init__(
        self,
        incidents_dir: str = INCIDENTS_DIR,
        cooldown_s: int = FLIGHT_COOLDOWN_S,
    ) -> None:
        self.incidents_dir = incidents_dir
        self.cooldown_s = cooldown_s
        self._ring: deque[dict] = deque()  # Time-based rolling window (no maxlen)
        self._capturing = False  # State machine: normal -> capturing -> finalize
        self._capture_buffer: list[dict] = []
        self._capture_remaining = 0
        self._last_incident_time = 0.0
        self._incident_reason = ""
        self._incident_count = 0

    def _prune_old_events(self, now_ms: int) -> None:
        """Remove events older than 10 minutes from the ring buffer."""
        cutoff_ms = now_ms - (FLIGHT_WINDOW_SECONDS * 1000)
        while self._ring and self._ring[0].get("ingest_ts_ms", 0) < cutoff_ms:
            self._ring.popleft()

    def record(self, event: dict) -> None:
        """Always feed ring buffer (rolling 10-min window). Also capture during incident."""
        now_ms = event.get("ingest_ts_ms", 0) or int(time.time() * 1000)

        # Always add to ring buffer (continuous rolling window)
        self._ring.append(event)
        self._prune_old_events(now_ms)

        # If capturing, also add to capture buffer
        if self._capturing:
            self._capture_buffer.append(event)
            self._capture_remaining -= 1
            if self._capture_remaining <= 0:
                self._finalize_incident()

    def trigger(self, reason: str) -> bool:
        """Trigger incident capture. Returns True if triggered, False if on cooldown/already capturing."""
        now = time.monotonic()
        if self._capturing:
            return False  # Already capturing, ignore
        if (now - self._last_incident_time) < self.cooldown_s:
            return False  # Cooldown to prevent spam

        # Start capture: take last N events from ring buffer + collect M post events
        self._capturing = True
        # Take at most FLIGHT_PRE_EVENTS from the 10-minute rolling window
        ring_list = list(self._ring)
        self._capture_buffer = ring_list[-FLIGHT_PRE_EVENTS:] if len(ring_list) > FLIGHT_PRE_EVENTS else ring_list
        self._capture_remaining = FLIGHT_POST_EVENTS
        self._incident_reason = reason
        self._last_incident_time = now
        log.warning(f"Incident triggered: {reason}")
        return True

    def _finalize_incident(self) -> None:
        """Write captured events + metadata to disk, then reset."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        incident_id = f"{ts}_{uuid.uuid4().hex[:8]}"
        incident_dir = os.path.join(self.incidents_dir, incident_id)

        try:
            os.makedirs(incident_dir, exist_ok=True)

            # Write all captured events (pre + post) to JSONL
            events_path = os.path.join(incident_dir, "events.jsonl")
            with open(events_path, "wb") as f:
                for event in self._capture_buffer:
                    f.write(orjson.dumps(event) + b"\n")

            # Write metadata for incident context
            total_events = len(self._capture_buffer)
            pre_count = min(total_events, FLIGHT_PRE_EVENTS)
            post_count = max(0, total_events - pre_count)
            meta = {
                "incident_id": incident_id,
                "reason": self._incident_reason,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "pre_events": pre_count,
                "post_events": post_count,
                "total_events": total_events,
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

        # Reset to normal operation
        self._capturing = False
        self._capture_buffer = []
        # Keep ring buffer intact for overlapping incidents - don't clear

    @property
    def incidents_captured(self) -> int:
        return self._incident_count


async def consumer_forensics(bus: BusLike, print_every_s: float = 10.0) -> None:
    """Main forensics pipeline: monitors data quality, detects anomalies, captures incidents."""
    q = bus.subscribe(maxsize=5000)
    counters = ForensicsCounters()

    # Initialize all detection/monitoring systems
    integrity_tracker = IntegrityTracker()
    latency_detector = LatencySpikeDetector()
    drift_writer = DriftSampleWriter(DRIFT_SAMPLE_FILE)
    flight_recorder = FlightRecorder()

    await drift_writer.start()

    last_print = time.monotonic()

    while True:
        event = await q.get()
        counters.processed += 1

        # Track P1->P2 latency for metrics
        ingest_ts = event.get("ingest_ts_ms", 0)
        recv_ts = event.get("recv_ts_ms", 0)
        latency_ms = (recv_ts - ingest_ts) if (ingest_ts and recv_ts) else None
        record_event(latency_ms)

        flight_recorder.record(event)  # Always feed flight recorder

        # Schema drift detection
        drift_result = check_schema_drift(event)
        if drift_result.is_drift:
            counters.drift += 1
            drift_writer.write(event, drift_result)  # Sample drift events to disk
            record_anomaly("drift")

        # Data integrity checks (per-symbol)
        is_dup, is_ooo, is_gap = integrity_tracker.check(event)
        if is_dup:
            counters.duplicates += 1
            record_anomaly("duplicate")
        if is_ooo:
            counters.out_of_order += 1
            record_anomaly("ooo")
        if is_gap:
            counters.gaps += 1
            record_anomaly("gap")

        # Latency spike detection (p99 over rolling window)
        if ingest_ts and recv_ts:
            is_spike = latency_detector.add_sample(ingest_ts, recv_ts)
            if is_spike:
                counters.spikes += 1
                record_anomaly("latency_spike")
                triggered = flight_recorder.trigger(
                    f"latency_spike_p99={latency_detector.get_p99()}ms"
                )
                if triggered:
                    counters.incidents = flight_recorder.incidents_captured
                    record_incident()

        # Trigger incident captures on critical anomalies
        if is_dup:
            triggered = flight_recorder.trigger("duplicate_detected")
            counters.incidents = flight_recorder.incidents_captured
            if triggered:
                record_incident()

        if is_gap:
            triggered = flight_recorder.trigger("sequence_gap")
            counters.incidents = flight_recorder.incidents_captured
            if triggered:
                record_incident()

        # Periodic stats logging
        now = time.monotonic()
        if (now - last_print) >= print_every_s:
            log.log(
                "FORENSICS",
                f"processed={counters.processed} | drift={counters.drift} | "
                f"dup={counters.duplicates} | ooo={counters.out_of_order} | gaps={counters.gaps} | "
                f"spikes={counters.spikes} | incidents={counters.incidents}"
            )
            last_print = now
