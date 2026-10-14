import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt


def main():
    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python3 plot_throughput.py path/to/throughput.csv"
        )

    csv_path = Path(sys.argv[1])

    with csv_path.open(newline="") as file:
        rows = list(csv.DictReader(file))

    if not rows:
        raise SystemExit("The throughput CSV contains no data.")

    starts = [float(row["bucketStartSeconds"]) for row in rows]
    ends = [float(row["bucketEndSeconds"]) for row in rows]
    rates = [float(row["throughput"]) for row in rows]

    # Plot each bucket's average at its midpoint.
    midpoints = [(start + end) / 2 for start, end in zip(starts, ends)]

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(midpoints, rates, marker="o", linewidth=2)

    ax.set_title("SkiTrak Ingest Throughput — EC2, 64 Threads")
    ax.set_xlabel("Elapsed ingest time (seconds)")
    ax.set_ylabel("Successful swipes per second")
    ax.set_xlim(0, ends[-1])
    ax.set_ylim(0, max(rates) * 1.20)
    ax.grid(True, alpha=0.3)

    # for midpoint, rate, start, end in zip(midpoints, rates, starts, ends):
    #     ax.annotate(
    #         f"{start:.1f}–{end:.1f}s",
    #         (midpoint, rate),
    #         xytext=(0, -20),
    #         textcoords="offset points",
    #         ha="center",
    #         fontsize=9,
    #     )

    fig.text(
        0.5,
        0.02,
        "10-second buckets; final partial bucket uses its actual duration. "
        "Includes warmup.",
        ha="center",
        fontsize=9,
    )

    fig.tight_layout(rect=(0, 0.05, 1, 1))

    output_path = csv_path.parent / "throughput.png"
    fig.savefig(output_path, dpi=200)
    plt.close(fig)

    print(f"Saved chart: {output_path.resolve()}")


if __name__ == "__main__":
    main()