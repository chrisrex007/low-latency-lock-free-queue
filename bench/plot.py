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
    """Load bench_results.csv and return list of row dicts."""
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "queue_type": row["queue_type"],
                "msg_bytes":  int(row["msg_bytes"]),
                "consumers":  int(row["consumers"]),
                "ops":        int(row["ops"]),
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

    print("Done.")


if __name__ == "__main__":
    main()
