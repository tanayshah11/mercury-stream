"""
Incident Report Generator for MercuryStream.

Generates markdown reports from incident bundles (meta.json + events.jsonl).
"""

import argparse
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import orjson
from loguru import logger as log


@dataclass
class IncidentReport:
    """Structured incident report data."""

    incident_id: str
    reason: str
    timestamp: str
    total_events: int
    pre_events: int
    post_events: int

    # Derived analysis
    affected_symbols: list[str] = field(default_factory=list)
    duration_ms: int = 0
    first_event_time: str = ""
    last_event_time: str = ""

    # Latency stats
    latency_min_ms: int = 0
    latency_max_ms: int = 0
    latency_avg_ms: float = 0.0
    latency_p99_ms: int = 0

    # Evidence
    duplicate_trade_ids: list[int] = field(default_factory=list)
    out_of_order_count: int = 0
    sequence_gaps: list[tuple[int, int]] = field(default_factory=list)

    # Sample events
    sample_events: list[dict] = field(default_factory=list)
    duplicate_samples: list[dict] = field(default_factory=list)


def analyze_events(events: list[dict]) -> dict:
    """Analyze events for anomalies and statistics."""
    if not events:
        return {}

    # Basic stats
    symbols = set()
    latencies = []
    trade_ids: dict[int, list[dict]] = defaultdict(list)
    timestamps_by_symbol: dict[str, list[int]] = defaultdict(list)
    sequences_by_symbol: dict[str, list[int]] = defaultdict(list)

    for event in events:
        symbol = event.get("product_id", "unknown")
        symbols.add(symbol)

        # Latency
        ingest_ts = event.get("ingest_ts_ms", 0)
        recv_ts = event.get("recv_ts_ms", 0)
        if ingest_ts and recv_ts:
            latency = recv_ts - ingest_ts
            if latency >= 0:
                latencies.append(latency)

        # Track trade IDs for duplicate detection
        trade_id = event.get("trade_id")
        if trade_id is not None:
            trade_ids[trade_id].append(event)

        # Track timestamps for OOO detection
        time_str = event.get("time", "")
        if time_str:
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                timestamps_by_symbol[symbol].append(ts_ms)
            except (ValueError, AttributeError):
                pass

        # Track sequences for gap detection
        seq = event.get("sequence")
        if seq is not None:
            sequences_by_symbol[symbol].append(seq)

    # Find duplicates
    duplicates = {tid: evts for tid, evts in trade_ids.items() if len(evts) > 1}

    # Find out-of-order
    ooo_count = 0
    for symbol, timestamps in timestamps_by_symbol.items():
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i - 1]:
                ooo_count += 1

    # Find sequence gaps
    gaps = []
    for symbol, seqs in sequences_by_symbol.items():
        sorted_seqs = sorted(set(seqs))
        for i in range(1, len(sorted_seqs)):
            if sorted_seqs[i] > sorted_seqs[i - 1] + 1:
                gaps.append((sorted_seqs[i - 1], sorted_seqs[i]))

    # Latency stats
    latency_stats = {}
    if latencies:
        sorted_lat = sorted(latencies)
        latency_stats = {
            "min": sorted_lat[0],
            "max": sorted_lat[-1],
            "avg": sum(latencies) / len(latencies),
            "p99": (
                sorted_lat[int(len(sorted_lat) * 0.99)]
                if len(sorted_lat) > 1
                else sorted_lat[0]
            ),
        }

    # Time range
    time_range = {}
    if events:
        times = []
        for e in events:
            t = e.get("time", "")
            if t:
                times.append(t)
        if times:
            time_range = {"first": min(times), "last": max(times)}

    return {
        "symbols": list(symbols),
        "duplicates": duplicates,
        "ooo_count": ooo_count,
        "gaps": gaps[:10],  # Limit to first 10
        "latency": latency_stats,
        "time_range": time_range,
    }


def load_incident(incident_dir: str) -> tuple[dict, list[dict]]:
    """Load meta.json and events.jsonl from incident directory."""
    meta_path = os.path.join(incident_dir, "meta.json")
    events_path = os.path.join(incident_dir, "events.jsonl")

    # Load meta
    with open(meta_path, "rb") as f:
        meta = orjson.loads(f.read())

    # Load events
    events = []
    with open(events_path, "rb") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(orjson.loads(line))
                except orjson.JSONDecodeError:
                    pass

    return meta, events


def build_report(incident_dir: str) -> IncidentReport:
    """Build an IncidentReport from an incident directory."""
    meta, events = load_incident(incident_dir)
    analysis = analyze_events(events)

    report = IncidentReport(
        incident_id=meta.get("incident_id", "unknown"),
        reason=meta.get("reason", "unknown"),
        timestamp=meta.get("timestamp", ""),
        total_events=meta.get("total_events", len(events)),
        pre_events=meta.get("pre_events", 0),
        post_events=meta.get("post_events", 0),
    )

    # Fill in analysis
    report.affected_symbols = analysis.get("symbols", [])

    if analysis.get("time_range"):
        report.first_event_time = analysis["time_range"].get("first", "")
        report.last_event_time = analysis["time_range"].get("last", "")
        # Calculate duration
        try:
            first_dt = datetime.fromisoformat(
                report.first_event_time.replace("Z", "+00:00")
            )
            last_dt = datetime.fromisoformat(
                report.last_event_time.replace("Z", "+00:00")
            )
            report.duration_ms = int((last_dt - first_dt).total_seconds() * 1000)
        except (ValueError, AttributeError):
            pass

    if analysis.get("latency"):
        lat = analysis["latency"]
        report.latency_min_ms = lat.get("min", 0)
        report.latency_max_ms = lat.get("max", 0)
        report.latency_avg_ms = lat.get("avg", 0.0)
        report.latency_p99_ms = lat.get("p99", 0)

    # Duplicates
    duplicates = analysis.get("duplicates", {})
    report.duplicate_trade_ids = list(duplicates.keys())[:10]
    for tid, evts in list(duplicates.items())[:3]:
        report.duplicate_samples.extend(evts[:2])

    report.out_of_order_count = analysis.get("ooo_count", 0)
    report.sequence_gaps = analysis.get("gaps", [])

    # Sample events (first 5 + last 5)
    if events:
        report.sample_events = events[:5] + events[-5:] if len(events) > 10 else events

    return report


def format_event_json(event: dict) -> str:
    """Format an event as indented JSON."""
    return orjson.dumps(event, option=orjson.OPT_INDENT_2).decode()


def generate_report(incident_dir: str, include_llm: bool = False) -> str:
    """Generate a markdown report for an incident."""
    report = build_report(incident_dir)

    # Format timestamp
    try:
        dt = datetime.fromisoformat(report.timestamp)
        formatted_ts = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, AttributeError):
        formatted_ts = report.timestamp

    # Build markdown
    lines = [
        f"# Incident Report: {report.incident_id}",
        "",
        "## Summary",
        "",
        "| Field | Value |",
        "|-------|-------|",
        f"| **Type** | `{report.reason}` |",
        f"| **Triggered** | {formatted_ts} |",
        f"| **Duration** | {report.duration_ms:,}ms |",
        f"| **Affected Symbols** | {', '.join(report.affected_symbols) or 'N/A'} |",
        f"| **Total Events** | {report.total_events:,} ({report.pre_events:,} pre + {max(0, report.post_events):,} post) |",
        "",
        "## Latency Stats",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Min | {report.latency_min_ms}ms |",
        f"| Max | {report.latency_max_ms}ms |",
        f"| Avg | {report.latency_avg_ms:.1f}ms |",
        f"| p99 | {report.latency_p99_ms}ms |",
        "",
    ]

    # Trigger context based on reason
    lines.extend(
        [
            "## Trigger Context",
            "",
        ]
    )

    if "duplicate" in report.reason.lower():
        if report.duplicate_trade_ids:
            lines.append(
                f"- **Cause:** Duplicate trade_id detected: `{report.duplicate_trade_ids[0]}`"
            )
        else:
            lines.append("- **Cause:** Duplicate event detected")
        lines.append(f"- **Total duplicates found:** {len(report.duplicate_trade_ids)}")

    elif "gap" in report.reason.lower() or "sequence" in report.reason.lower():
        if report.sequence_gaps:
            gap = report.sequence_gaps[0]
            lines.append(
                f"- **Cause:** Sequence gap detected between `{gap[0]}` and `{gap[1]}`"
            )
        else:
            lines.append("- **Cause:** Sequence gap detected")
        lines.append(f"- **Total gaps found:** {len(report.sequence_gaps)}")

    elif "latency" in report.reason.lower() or "spike" in report.reason.lower():
        lines.append(
            f"- **Cause:** Latency spike detected (p99 = {report.latency_p99_ms}ms)"
        )

    elif "drift" in report.reason.lower():
        lines.append("- **Cause:** Schema drift detected")

    else:
        lines.append(f"- **Cause:** {report.reason}")

    lines.append(f"- **Latency p99 at trigger:** {report.latency_p99_ms}ms")
    if report.out_of_order_count > 0:
        lines.append(f"- **Out-of-order events:** {report.out_of_order_count}")
    lines.append("")

    # Evidence samples
    lines.extend(
        [
            "## Evidence Samples",
            "",
        ]
    )

    if report.duplicate_samples:
        lines.append("### Duplicate Events")
        lines.append("```json")
        for event in report.duplicate_samples[:4]:
            lines.append(orjson.dumps(event).decode())
        lines.append("```")
        lines.append("")

    if report.sequence_gaps:
        lines.append("### Sequence Gaps")
        for gap in report.sequence_gaps[:5]:
            lines.append(
                f"- Gap between sequence `{gap[0]}` and `{gap[1]}` (missing {gap[1] - gap[0] - 1} events)"
            )
        lines.append("")

    # Sample events
    lines.extend(
        [
            "### Sample Events (first 5)",
            "```json",
        ]
    )
    for event in report.sample_events[:5]:
        lines.append(orjson.dumps(event).decode())
    lines.extend(
        [
            "```",
            "",
        ]
    )

    # Reproduce command
    lines.extend(
        [
            "## Reproduce",
            "",
            "```bash",
            f"python replay.py --file data/incidents/{report.incident_id}/events.jsonl --rate 500",
            "```",
            "",
        ]
    )

    # Placeholder for LLM analysis
    if include_llm:
        llm_path = os.path.join(incident_dir, "ai_analysis.md")
        if os.path.exists(llm_path):
            lines.extend(
                [
                    "---",
                    "",
                    "## AI Analysis",
                    "",
                ]
            )
            with open(llm_path, "r") as f:
                lines.append(f.read())

    return "\n".join(lines)


def process_incidents_dir(incidents_dir: str) -> None:
    """Process all incidents in a directory and generate reports."""
    incidents_path = Path(incidents_dir)

    if not incidents_path.exists():
        log.error(f"Incidents directory not found: {incidents_dir}")
        return

    incident_dirs = [d for d in incidents_path.iterdir() if d.is_dir()]

    if not incident_dirs:
        log.error("No incidents found.")
        return

    log.info(f"Found {len(incident_dirs)} incident(s)\n")

    for incident_dir in sorted(incident_dirs):
        meta_path = incident_dir / "meta.json"
        if not meta_path.exists():
            continue

        log.info(f"Processing: {incident_dir.name}")

        try:
            report_md = generate_report(str(incident_dir))

            # Write report
            report_path = incident_dir / "report.md"
            with open(report_path, "w") as f:
                f.write(report_md)

            log.info(f"Generated: {report_path}")

            # Print summary to stdout
            with open(meta_path, "rb") as f:
                meta = orjson.loads(f.read())
            log.info(f"Type of incident: {meta.get('reason', 'unknown')}")
            log.info(f"Total events: {meta.get('total_events', 0)}")

        except Exception as e:
            log.error(f"Error: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate incident reports from MercuryStream incident bundles"
    )
    parser.add_argument(
        "path",
        help="Path to incident directory or incidents parent directory",
    )
    parser.add_argument(
        "--include-llm",
        action="store_true",
        help="Include LLM analysis if available",
    )

    args = parser.parse_args()
    path = Path(args.path)

    # Check if this is a single incident or a directory of incidents
    if (path / "meta.json").exists():
        # Single incident
        log.info(f"Processing single incident: {path}")
        report_md = generate_report(str(path), include_llm=args.include_llm)
        report_path = path / "report.md"
        with open(report_path, "w") as f:
            f.write(report_md)
        log.info(f"Generated: {report_path}")
        log.info(report_md)
    else:
        # Directory of incidents
        log.info(f"Processing directory: {path}")
        process_incidents_dir(str(path))


if __name__ == "__main__":
    main()
