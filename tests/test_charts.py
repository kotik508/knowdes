"""Charts of the measurements (#15)."""

import json
import sys
import xml.etree.ElementTree as ET

from knowdes import charts


def result(run, label, **values):
    base = {"run": run, "label": label, "per_second": 0, "p50_ms": 0, "p95_ms": 0, "mean_ms": 0}
    return {**base, **values}


SAMPLE = [
    result("writes", "3 nodes", clients=1, per_second=100.0),
    result("writes", "3 nodes", clients=8, per_second=400.0, p50_ms=12.0, p95_ms=30.0),
    result("writes", "5 nodes", clients=8, per_second=300.0, p50_ms=15.0, p95_ms=40.0),
    result("reads leader", "3 nodes", p50_ms=3.0),
    result("reads follower", "3 nodes", p50_ms=1.0),
    result("failover", "3 nodes", shard=0, mean_ms=300.0, p95_ms=400.0),
    result("failover", "3 nodes", shard=1, mean_ms=500.0, p95_ms=600.0),
]


def test_every_chart_is_valid_svg():
    out = charts.charts(SAMPLE)
    assert sorted(out) == ["failover.svg", "latency.svg", "reads.svg", "writes.svg"]
    for svg in out.values():
        root = ET.fromstring(svg)
        assert root.tag.endswith("svg")


def test_values_are_averaged_by_cluster_size():
    assert charts.pick(SAMPLE, "failover", "mean_ms") == {"3 nodes": 400.0}
    assert charts.pick(SAMPLE, "writes", "per_second", clients=8) == {
        "3 nodes": 400.0,
        "5 nodes": 300.0,
    }


def test_bar_heights_follow_the_values():
    svg = charts.charts(SAMPLE)["writes.svg"]
    heights = [
        float(r.get("height"))
        for r in ET.fromstring(svg).iter("{http://www.w3.org/2000/svg}rect")
        if r.find("{http://www.w3.org/2000/svg}title") is not None
    ]
    # 100 and 400 writes per second with 3 nodes, 300 with 5 nodes.
    assert heights[1] == 4 * heights[0]
    assert abs(heights[2] - 3 * heights[0]) < 0.2


def test_command_line(tmp_path):
    path = tmp_path / "r.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in SAMPLE))
    old = sys.argv
    sys.argv = ["charts", str(path), str(tmp_path / "out")]
    try:
        charts.main()
    finally:
        sys.argv = old
    assert len(list((tmp_path / "out").glob("*.svg"))) == 4


def test_axis_ticks_are_round():
    for top, step in ((15, 5), (150, 50), (600, 200), (1000, 200)):
        assert top / charts.ticks(top) == step
