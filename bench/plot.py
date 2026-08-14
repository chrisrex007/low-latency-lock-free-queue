#!/usr/bin/env python3
"""bench/plot.py — Generate throughput chart from bench_results.csv."""

import csv
import sys
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("Error: matplotlib is required. Install with: pip install matplotlib",
          file=sys.stderr)
    sys.exit(1)


def load_csv(path: Path) -> list[dict]:
    """Load bench_results.csv and return list of row dicts.

    `producers` and `pinned` are read defensively so that a CSV generated
    before those columns existed still loads.
    """
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "queue_type": row["queue_type"],
                "msg_bytes":  int(row["msg_bytes"]),
                "producers":  int(row.get("producers", 1)),
                "consumers":  int(row["consumers"]),
                "ops":        int(row["ops"]),
                "pinned":     int(row.get("pinned", 0)),
                "median_mops": float(row["median_mops"]),
            })
    return rows


# ─── Chart: Throughput vs Message Size (SPSC vs Mutex) ───────────────────────

def plot_throughput_vs_msgsize(rows: list[dict], out_path: Path):
    """Line chart: throughput at 1 consumer for SPSC lock-free vs mutex."""
    queue_labels = {
        "spsc_lockfree": "SPSC (lock-free)",
        "mutex_spsc":    "Mutex (baseline)",
    }

    # Collect data: queue_type -> {msg_bytes: median_mops}
    data = defaultdict(dict)
    for r in rows:
        if r["consumers"] != 1:
            continue
        qt = r["queue_type"]
        if qt not in queue_labels:
            continue
        data[qt][r["msg_bytes"]] = r["median_mops"]

    msg_sizes = sorted({r["msg_bytes"] for r in rows})

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = {
        "spsc_lockfree": "#2ecc71",
        "mutex_spsc":    "#e74c3c",
    }
    markers = {
        "spsc_lockfree": "o",
        "mutex_spsc":    "^",
    }

    x_labels = [f"{s}B" for s in msg_sizes]
    x_pos = list(range(len(msg_sizes)))

    for qt, label in queue_labels.items():
        vals = [data[qt].get(s, 0) for s in msg_sizes]
        ax.plot(x_pos, vals, marker=markers[qt], label=label,
                color=colors[qt], linewidth=2, markersize=8)

    ax.set_xlabel("Message Size", fontsize=12)
    ax.set_ylabel("Throughput (Mops/s)", fontsize=12)
    ax.set_title("Throughput vs Message Size  (SPSC lock-free vs Mutex)",
                 fontsize=14, fontweight="bold")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels)
    ax.legend(fontsize=11)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ─── Chart: Throughput vs Thread Count (MPMC lock-free vs Mutex) ─────────────

# Representative message size for the scaling chart. 64B is small enough that
# the run is bound by contention and cache traffic rather than by memcpy cost,
# which is the behaviour the scaling comparison is meant to show.
THREADS_CHART_MSG_BYTES = 64


def plot_throughput_vs_threads(rows: list[dict], out_path: Path):
    """Line chart: throughput across producer/consumer thread counts."""
    queue_labels = {
        "mpmc_lockfree": "MPMC (lock-free)",
        "mutex_mpmc":    "Mutex (baseline)",
    }
    colors = {
        "mpmc_lockfree": "#3498db",
        "mutex_mpmc":    "#e74c3c",
    }
    markers = {
        "mpmc_lockfree": "o",
        "mutex_mpmc":    "^",
    }

    selected = [r for r in rows
                if r["queue_type"] in queue_labels
                and r["msg_bytes"] == THREADS_CHART_MSG_BYTES]

    if not selected:
        print(f"Warning: no MPMC rows at {THREADS_CHART_MSG_BYTES}B in the CSV — "
              f"skipping {out_path.name}. (Regenerate with 'make run-bench'.)",
              file=sys.stderr)
        return

    # Derive the x-axis from the data rather than hardcoding the config list:
    # the benchmark sizes its sweep from hardware_concurrency(), so the set of
    # configs differs between machines and a hardcoded list would drop points.
    configs = sorted({(r["producers"], r["consumers"]) for r in selected},
                     key=lambda pc: (pc[0] + pc[1], pc[0]))
    x_pos = list(range(len(configs)))
    x_labels = [f"{p}P{c}C" for p, c in configs]

    # queue_type -> {(producers, consumers): median_mops}
    data = defaultdict(dict)
    for r in selected:
        data[r["queue_type"]][(r["producers"], r["consumers"])] = r["median_mops"]

    fig, ax = plt.subplots(figsize=(10, 6))

    for qt, label in queue_labels.items():
        vals = [data[qt].get(cfg) for cfg in configs]
        ax.plot(x_pos, vals, marker=markers[qt], label=label,
                color=colors[qt], linewidth=2, markersize=8)

    pinned = {r["pinned"] for r in selected}
    pin_note = ("threads pinned" if pinned == {1}
                else "unpinned" if pinned == {0}
                else "mixed pinning")

    ax.set_xlabel("Producer / Consumer Threads", fontsize=12)
    ax.set_ylabel("Throughput (Mops/s)", fontsize=12)
    ax.set_title(
        f"Throughput vs Thread Count  (MPMC lock-free vs Mutex, "
        f"{THREADS_CHART_MSG_BYTES}B messages, {pin_note})",
        fontsize=13, fontweight="bold")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels)
    ax.legend(fontsize=11)
    ax.grid(axis="y", alpha=0.3)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved: {out_path}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    repo = Path(__file__).resolve().parent.parent
    csv_path = repo / "bench_results.csv"

    if not csv_path.exists():
        print(f"Error: {csv_path} not found. Run 'make run-bench' first.",
              file=sys.stderr)
        sys.exit(1)

    rows = load_csv(csv_path)

    plot_throughput_vs_msgsize(rows, repo / "throughput_vs_msgsize.png")
    plot_throughput_vs_threads(rows, repo / "throughput_vs_threads.png")

    print("Done.")


if __name__ == "__main__":
    main()
