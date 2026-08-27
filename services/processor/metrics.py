"""
Prometheus metrics for MercuryStream P2.

Exposes metrics via HTTP /metrics endpoint for Prometheus scraping.
"""

import asyncio
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread, Lock
from typing import Callable

# Thread-safe metrics lock - protects all metric updates
_metrics_lock = Lock()

# Metrics state - all updates must be protected by _metrics_lock
# Stores all metric values - counters accumulate, gauges are snapshots
_metrics = {
    "events_total": 0,  # counter: total events processed
    "events_per_second": 0.0,  # gauge: calculated rate
    "drops_total": 0,  # counter: events dropped
    "anomalies_duplicate": 0,  # counter: duplicate seq numbers
    "anomalies_ooo": 0,  # counter: out-of-order events
    "anomalies_gap": 0,  # counter: missing seq numbers
    "anomalies_drift": 0,  # counter: schema changes
    "anomalies_latency_spike": 0,  # counter: sudden latency increases
    "incidents_total": 0,  # counter: incident snapshots saved
    "latency_sum_ms": 0,  # histogram: sum for avg calculation
    "latency_count": 0,  # histogram: total observations
    "latency_min_ms": 0,  # gauge: track min latency seen
    "latency_max_ms": 0,  # gauge: track max latency seen
    "queue_depth_max": 0,  # gauge: max consumer queue depth
}

# Latency histogram buckets (in ms): defines distribution ranges
# Each bucket counts observations <= that value (e.g., bucket 10 = all latencies from 0-10ms)
_latency_buckets = [1, 5, 10, 25, 50, 100, 250, 500, 1000, float("inf")]
_latency_histogram: dict[float, int] = {b: 0 for b in _latency_buckets}

# Rate calculation state: tracks delta between updates
_last_events_total = 0  # snapshot of events_total from last calculation
_last_rate_time = time.monotonic()  # monotonic clock for accurate intervals

METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))


def inc(name: str, value: int = 1) -> None:
    """Increment a counter metric (thread-safe)."""
    with _metrics_lock:
        _metrics[name] = _metrics.get(name, 0) + value


def set_gauge(name: str, value: float) -> None:
    """Set a gauge metric (thread-safe)."""
    with _metrics_lock:
        _metrics[name] = value


def observe_latency(latency_ms: int) -> None:
    """Record a latency observation for histogram (thread-safe)."""
    with _metrics_lock:
        # Track sum & count for average calculation
        _metrics["latency_sum_ms"] += latency_ms
        _metrics["latency_count"] += 1

        # Update min/max bounds (0 means uninitialized)
        if _metrics["latency_min_ms"] == 0 or latency_ms < _metrics["latency_min_ms"]:
            _metrics["latency_min_ms"] = latency_ms
        if latency_ms > _metrics["latency_max_ms"]:
            _metrics["latency_max_ms"] = latency_ms

        # Increment first matching bucket (creates distribution)
        # Only increments one bucket per observation to avoid double-counting
        for bucket in _latency_buckets:
            if latency_ms <= bucket:
                _latency_histogram[bucket] += 1
                break


def update_rate() -> None:
    """Update events per second rate (thread-safe)."""
    global _last_events_total, _last_rate_time

    now = time.monotonic()

    with _metrics_lock:
        elapsed = now - _last_rate_time

        # Only recalculate rate every 1+ seconds to avoid jitter
        if elapsed >= 1.0:
            current_total = _metrics["events_total"]
            rate = (current_total - _last_events_total) / elapsed  # delta / time = rate
            _metrics["events_per_second"] = rate
            _last_events_total = current_total
            _last_rate_time = now


def get_prometheus_metrics() -> str:
    """Generate Prometheus-format metrics output (thread-safe)."""
    update_rate()  # refresh rate calculation before export

    # Take snapshot of metrics under lock to ensure consistency
    with _metrics_lock:
        metrics_snapshot = _metrics.copy()
        histogram_snapshot = _latency_histogram.copy()

    # Prometheus exposition format: HELP, TYPE, then metric lines
    lines = [
        "# HELP mercurystream_events_total Total events processed",
        "# TYPE mercurystream_events_total counter",
        f"mercurystream_events_total {metrics_snapshot['events_total']}",
        "",
        "# HELP mercurystream_events_per_second Current events per second",
        "# TYPE mercurystream_events_per_second gauge",
        f"mercurystream_events_per_second {metrics_snapshot['events_per_second']:.2f}",
        "",
        "# HELP mercurystream_drops_total Total dropped events",
        "# TYPE mercurystream_drops_total counter",
        f"mercurystream_drops_total {metrics_snapshot['drops_total']}",
        "",
        "# HELP mercurystream_anomalies_total Total anomalies detected by type",
        "# TYPE mercurystream_anomalies_total counter",
        # Labels {type="..."} allow grouping different anomaly types under same metric
        f'mercurystream_anomalies_total{{type="duplicate"}} {metrics_snapshot["anomalies_duplicate"]}',
        f'mercurystream_anomalies_total{{type="out_of_order"}} {metrics_snapshot["anomalies_ooo"]}',
        f'mercurystream_anomalies_total{{type="sequence_gap"}} {metrics_snapshot["anomalies_gap"]}',
        f'mercurystream_anomalies_total{{type="schema_drift"}} {metrics_snapshot["anomalies_drift"]}',
        f'mercurystream_anomalies_total{{type="latency_spike"}} {metrics_snapshot["anomalies_latency_spike"]}',
        "",
        "# HELP mercurystream_incidents_total Total incidents captured",
        "# TYPE mercurystream_incidents_total counter",
        f"mercurystream_incidents_total {metrics_snapshot['incidents_total']}",
        "",
        "# HELP mercurystream_latency_ms Event latency histogram",
        "# TYPE mercurystream_latency_ms histogram",
    ]

    # Histogram buckets must be cumulative (each bucket includes all smaller ones)
    # Prometheus uses this to calculate quantiles (p50, p95, p99)
    cumulative = 0
    for bucket in _latency_buckets:
        cumulative += histogram_snapshot[bucket]  # sum all observations up to this bucket
        if bucket == float("inf"):
            lines.append(f'mercurystream_latency_ms_bucket{{le="+Inf"}} {cumulative}')
        else:
            lines.append(f'mercurystream_latency_ms_bucket{{le="{bucket}"}} {cumulative}')

    # Histogram requires _sum and _count for average calculation
    lines.extend([
        f"mercurystream_latency_ms_sum {metrics_snapshot['latency_sum_ms']}",
        f"mercurystream_latency_ms_count {metrics_snapshot['latency_count']}",
        "",
        "# HELP mercurystream_queue_depth_max Maximum queue depth across consumers",
        "# TYPE mercurystream_queue_depth_max gauge",
        f"mercurystream_queue_depth_max {metrics_snapshot['queue_depth_max']}",
        "",
    ])

    return "\n".join(lines)  # newline-delimited text format


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for /metrics endpoint."""

    def do_GET(self):
        if self.path == "/metrics":
            content = get_prometheus_metrics()
            self.send_response(200)
            # Prometheus expects text/plain with UTF-8 encoding
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content.encode())
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress default HTTP logging - metrics scraping is noisy
        pass


def start_metrics_server(port: int = METRICS_PORT) -> None:
    """Start the metrics HTTP server in a background thread."""
    def run_server():
        server = HTTPServer(("0.0.0.0", port), MetricsHandler)
        server.serve_forever()  # blocks until shutdown

    # Daemon thread exits when main process exits
    thread = Thread(target=run_server, daemon=True)
    thread.start()


# Convenience functions for common operations
def record_event(latency_ms: int | None = None) -> None:
    """Record a processed event."""
    inc("events_total")
    if latency_ms is not None and latency_ms >= 0:
        observe_latency(latency_ms)


def record_drop() -> None:
    """Record a dropped event."""
    inc("drops_total")


def record_anomaly(anomaly_type: str) -> None:
    """Record an anomaly detection."""
    key = f"anomalies_{anomaly_type}"
    if key in _metrics:  # only increment if valid anomaly type
        inc(key)


def record_incident() -> None:
    """Record an incident capture."""
    inc("incidents_total")


def set_queue_depth(depth: int) -> None:
    """Set the current maximum queue depth."""
    set_gauge("queue_depth_max", depth)
