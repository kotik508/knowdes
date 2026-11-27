"""Charts of the measurements for the report (#15, design plan section 7).

Reads the JSON lines that knowdes.bench writes and draws one SVG per question,
without any plotting library:

    python -m knowdes.charts docs/measurements/laptop-2026-11-27.jsonl docs/measurements

writes.svg     write throughput with 1, 8 and 32 clients, by cluster size
latency.svg    write latency (median and 95th percentile) with 8 clients
reads.svg      leader reads against follower reads, median latency
failover.svg   time until a shard has a new leader after its leader stops
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import fmean
from xml.sax.saxutils import escape

COLORS = ("#3b6ea5", "#d08c2f", "#5a9a5a", "#9a5a8a")
WIDTH, HEIGHT = 640, 360
LEFT, RIGHT, TOP, BOTTOM = 64, 16, 40, 56


def load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def sizes(results: list[dict]) -> list[str]:
    """Cluster labels in the order of their node count, e.g. 3 nodes, 4 nodes."""
    return sorted({r["label"] for r in results}, key=lambda s: (len(s), s))


def pick(results: list[dict], run: str, value: str, **where) -> dict[str, float]:
    """Mean of one value by label, for the results of one kind of run."""
    found: dict[str, list[float]] = defaultdict(list)
    for r in results:
        if r["run"] == run and all(r.get(k) == v for k, v in where.items()):
            found[r["label"]].append(r[value])
    return {label: fmean(v) for label, v in found.items()}


def nice_max(value: float) -> float:
    """A round upper end for the axis."""
    if value <= 0:
        return 1.0
    step = 10 ** len(str(int(value))) / 10
    for factor in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if value <= step * factor:
            return step * factor
    return step * 10


def ticks(top: float) -> int:
    """Number of steps on the axis, so that every tick is a round number."""
    for steps in (3, 4, 5, 6):
        step = top / steps
        scale = 10 ** (len(str(int(step))) - 1) if step >= 1 else 0.1
        if abs(step / scale - round(step / scale)) < 1e-9 and round(step / scale) in (1, 2, 5):
            return steps
    return 4


def bars(title: str, unit: str, groups: list[str], series: dict[str, dict[str, float]]) -> str:
    """Grouped bar chart: one group per cluster size, one bar per series."""
    top = nice_max(max((v for s in series.values() for v in s.values()), default=0))
    plot_w, plot_h = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM
    group_w = plot_w / max(1, len(groups))
    bar_w = group_w * 0.7 / max(1, len(series))
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" font-family="sans-serif" font-size="12">',
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="white"/>',
        f'<text x="{LEFT}" y="22" font-size="14">{escape(title)}</text>',
    ]
    steps = ticks(top)
    for i in range(steps + 1):
        value = top * i / steps
        y = TOP + plot_h - plot_h * i / steps
        out.append(
            f'<line x1="{LEFT}" y1="{y:.1f}" x2="{WIDTH - RIGHT}" y2="{y:.1f}" stroke="#ddd"/>'
        )
        out.append(f'<text x="{LEFT - 6}" y="{y + 4:.1f}" text-anchor="end">{value:g}</text>')
    out.append(
        f'<text x="14" y="{TOP + plot_h / 2:.1f}" transform="rotate(-90 14 '
        f'{TOP + plot_h / 2:.1f})" text-anchor="middle">{escape(unit)}</text>'
    )
    for g, group in enumerate(groups):
        x0 = LEFT + g * group_w + group_w * 0.15
        for s, (name, values) in enumerate(series.items()):
            if group not in values:
                continue
            h = plot_h * values[group] / top
            x = x0 + s * bar_w
            y = TOP + plot_h - h
            out.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w - 2:.1f}" height="{h:.1f}" '
                f'fill="{COLORS[s % len(COLORS)]}"><title>{escape(name)}: '
                f"{values[group]:g}</title></rect>"
            )
            out.append(
                f'<text x="{x + (bar_w - 2) / 2:.1f}" y="{y - 4:.1f}" text-anchor="middle" '
                f'font-size="10">{values[group]:.3g}</text>'
            )
        out.append(
            f'<text x="{LEFT + (g + 0.5) * group_w:.1f}" y="{TOP + plot_h + 18}" '
            f'text-anchor="middle">{escape(group)}</text>'
        )
    for s, name in enumerate(series):
        x = LEFT + s * 150
        y = HEIGHT - 14
        out.append(
            f'<rect x="{x}" y="{y - 10}" width="10" height="10" fill="{COLORS[s % len(COLORS)]}"/>'
        )
        out.append(f'<text x="{x + 14}" y="{y}">{escape(name)}</text>')
    out.append("</svg>")
    return "\n".join(out) + "\n"


def charts(results: list[dict]) -> dict[str, str]:
    groups = sizes(results)
    writes = {
        f"{c} client{'s' if c > 1 else ''}": pick(results, "writes", "per_second", clients=c)
        for c in (1, 8, 32)
    }
    latency = {
        "median": pick(results, "writes", "p50_ms", clients=8),
        "95th percentile": pick(results, "writes", "p95_ms", clients=8),
    }
    reads = {
        "leader read": pick(results, "reads leader", "p50_ms"),
        "follower read": pick(results, "reads follower", "p50_ms"),
    }
    failover = {
        "mean": pick(results, "failover", "mean_ms"),
        "95th percentile": pick(results, "failover", "p95_ms"),
    }
    return {
        "writes.svg": bars("Write throughput", "writes per second", groups, writes),
        "latency.svg": bars("Write latency, 8 clients", "milliseconds", groups, latency),
        "reads.svg": bars("Read latency, median, 16 clients", "milliseconds", groups, reads),
        "failover.svg": bars("Time to a new leader", "milliseconds", groups, failover),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("results", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for name, svg in charts(load(args.results)).items():
        (args.out / name).write_text(svg)
        print(args.out / name)


if __name__ == "__main__":
    main()
