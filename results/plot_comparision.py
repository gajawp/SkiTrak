import csv
from pathlib import Path

import matplotlib.pyplot as plt

RESULTS = Path(__file__).resolve().parent

# Observed EC2 runs. The 32-thread run used Client Part 1;
# the other runs used Client Part 2 with latency instrumentation.
labels = ["32\nPart 1", "64", "128", "256\nFirst", "256\nRepeat"]
main_rates = [794.31, 1761.15, 2869.54, 3749.67, 6293.81]
overall_rates = [799.48, 1660.00, 2533.06, 2456.54, 4614.83]
predicted_rates = [1041.90, 2083.80, 4167.60, 8335.19, 8335.19]

plt.rcParams.update({
    "font.size": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

# Chart 1: compare every observed EC2 configuration.
fig, ax = plt.subplots(figsize=(11, 6))
positions = list(range(len(labels)))
width = 0.34

main_bars = ax.bar(
    [x - width / 2 for x in positions],
    main_rates,
    width,
    label="Main phase",
    color="#2563eb",
)

overall_bars = ax.bar(
    [x + width / 2 for x in positions],
    overall_rates,
    width,
    label="Overall ingest, including warmup",
    color="#14b8a6",
)

ax.plot(
    positions,
    predicted_rates,
    marker="D",
    linestyle="--",
    color="#d97706",
    label="Prediction from unloaded RTT",
)

ax.bar_label(main_bars, fmt="%.0f", padding=3, fontsize=9)
ax.bar_label(overall_bars, fmt="%.0f", padding=3, fontsize=9)

ax.set_xticks(positions, labels)
ax.set_xlabel("Main-phase sender threads / run")
ax.set_ylabel("Successful swipes per second")
ax.set_title("SkiTrak EC2 Throughput by Thread Count")
ax.set_ylim(0, max(predicted_rates) * 1.25)
ax.grid(axis="y", alpha=0.25)
ax.set_axisbelow(True)
ax.legend(loc="upper left")

fig.text(
    0.5,
    0.025,
    "500,000 swipes per run; warmup always uses 32 threads.\n"
    "32-thread result uses Part 1; others use Part 2. "
    "Sequential runs reflect changing conditions.",
    ha="center",
    fontsize=9,
)

fig.tight_layout(rect=(0, 0.09, 1, 1))
comparison_path = RESULTS / "ec2-thread-comparison.png"
fig.savefig(comparison_path, dpi=200)
plt.close(fig)

# Chart 2: latest 256-thread run, using its actual bucket CSV.
run_directory = RESULTS / "part2-256threads-1791755317589"
csv_path = run_directory / "throughput.csv"

with csv_path.open(newline="") as file:
    rows = list(csv.DictReader(file))

if not rows:
    raise SystemExit("The throughput CSV contains no data.")

starts = [float(row["bucketStartSeconds"]) for row in rows]
ends = [float(row["bucketEndSeconds"]) for row in rows]
rates = [float(row["throughput"]) for row in rows]
midpoints = [(start + end) / 2 for start, end in zip(starts, ends)]

fig, ax = plt.subplots(figsize=(11, 6))

ax.plot(
    midpoints,
    rates,
    marker="o",
    linewidth=2,
    color="#2563eb",
    label="Bucket throughput",
)

ax.axvline(
    33.985,
    color="#d97706",
    linestyle="--",
    linewidth=1.5,
    label="Warmup ends: 33.985 seconds",
)

ax.set_title("SkiTrak EC2 Throughput — 256 Threads, Repeat Run")
ax.set_xlabel("Elapsed ingest time (seconds)")
ax.set_ylabel("Successful swipes per second")
ax.set_xlim(0, ends[-1])
ax.set_ylim(0, max(rates) * 1.20)
ax.grid(alpha=0.25)
ax.legend(loc="upper left")

fig.text(
    0.5,
    0.025,
    "10-second buckets; final partial bucket uses its actual duration.\n"
    "The bucket crossing the warmup boundary includes both phases.",
    ha="center",
    fontsize=9,
)

fig.tight_layout(rect=(0, 0.09, 1, 1))
single_path = run_directory / "throughput-256.png"
fig.savefig(single_path, dpi=200)
plt.close(fig)

print(f"Saved: {comparison_path}")
print(f"Saved: {single_path}")