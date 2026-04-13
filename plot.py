import os
import sys

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

if len(sys.argv) < 2:
    print("Usage: python plot.py <telemetry_file.csv>")
    sys.exit(1)

csv_file = sys.argv[1]
if not os.path.exists(csv_file):
    print(f"File not found: {csv_file}")
    sys.exit(1)

df = pd.read_csv(csv_file)

df["Engine"] = df["Engine"].replace(
    {"HYBRID_ROCKSDB": "HYBRID_DB", "HYBRID_LMDB": "HYBRID_DB"}
)

# Filter out SYSTEM rows for standard telemetry graphs
telemetry_df = df[df["OpType"] != "SYSTEM"].copy()
# Extract events
events_df = df[df["Event"] != "NONE"].copy()

telemetry_df["RelativeTime"] = telemetry_df.groupby("Engine")["Timestamp"].transform(
    lambda x: x - x.min()
)

sns.set_theme(style="darkgrid")
plt.rcParams.update({"figure.autolayout": True})
fig, axes = plt.subplots(3, 2, figsize=(18, 16))
fig.suptitle(
    f"Database Engine Telemetry Analysis ({csv_file})", fontsize=20, fontweight="bold"
)

# Graph: Latency vs Time
sns.lineplot(
    data=telemetry_df,
    x="RelativeTime",
    y="AvgLatency_ms",
    hue="Engine",
    ax=axes[0, 0],
    alpha=0.7,
)
axes[0, 0].set_title("Avg Latency vs Time", fontsize=14)
axes[0, 0].set_ylabel("Latency (ms)")
axes[0, 0].set_yscale("log")  # Log scale usually better for latency spikes

# Graph: Throughput vs Time
sns.lineplot(
    data=telemetry_df,
    x="RelativeTime",
    y="OpsPerSec",
    hue="Engine",
    ax=axes[0, 1],
    alpha=0.7,
)
axes[0, 1].set_title("Throughput vs Time", fontsize=14)
axes[0, 1].set_ylabel("Ops / Sec")

# Graph: RAM Usage vs Time
sns.lineplot(
    data=telemetry_df,
    x="RelativeTime",
    y="RamMB",
    hue="Engine",
    ax=axes[1, 0],
    alpha=0.8,
)
axes[1, 0].set_title("RAM Usage vs Time", fontsize=14)
axes[1, 0].set_ylabel("RAM (MB)")

# Graph: Scan Latency Comparison (Bar Graph)
scan_df = telemetry_df[telemetry_df["OpType"] == "SCAN"]
sns.barplot(
    data=scan_df,
    x="Engine",
    y="AvgLatency_ms",
    ax=axes[1, 1],
    palette="muted",
    hue="Engine",
    legend=False,
)
axes[1, 1].set_title("Average Scan Latency Comparison", fontsize=14)
axes[1, 1].set_yscale("log")
axes[1, 1].set_ylabel("Avg Latency (ms)")

# Graph: Latency Distribution (Box Plot)
# Taking a sample so the box plot doesn't take 10 years to render if data is huge
sample_size = min(len(telemetry_df), 50000)
sampled_df = telemetry_df.sample(n=sample_size, random_state=42)
sns.boxplot(
    data=sampled_df,
    x="Engine",
    y="AvgLatency_ms",
    ax=axes[2, 0],
    palette="pastel",
    hue="Engine",
    legend=False,
)
axes[2, 0].set_title("Latency Distribution (P50/P95/P99)", fontsize=14)
axes[2, 0].set_yscale("log")
axes[2, 0].set_ylabel("Latency (ms) - Log Scale")

# Graph: Event Timeline Overlay
# We'll map Throughput and overlay the vertical event lines
sns.lineplot(
    data=telemetry_df,
    x="RelativeTime",
    y="OpsPerSec",
    hue="Engine",
    ax=axes[2, 1],
    alpha=0.5,
)
axes[2, 1].set_title("Throughput with Event Timeline Overlay", fontsize=14)

colors = {
    "MIGRATION_START": "blue",
    "MIGRATION_END": "green",
    "REVERT": "red",
    "MORPH_START": "purple",
    "MORPH_END": "magenta",
}

hybrid_start_time = df[df["Engine"].str.contains("HYBRID", na=False)]["Timestamp"].min()
for _, row in events_df.iterrows():
    event = str(row["Event"])
    adjusted_ts = row["Timestamp"] - hybrid_start_time
    # Default to black if event isn't in dictionary
    c = colors.get(event, "black")
    axes[2, 1].axvline(x=adjusted_ts, color=c, linestyle="--", linewidth=2, label=event)

# Deduplicate legend for vertical lines
handles, labels = axes[2, 1].get_legend_handles_labels()
by_label = dict(zip(labels, handles))
axes[2, 1].legend(
    by_label.values(), by_label.keys(), bbox_to_anchor=(1.05, 1), loc="upper left"
)

plt.tight_layout()
output_img = f"{csv_file.split('.')[0]}_dashboard.png"
plt.savefig(output_img, dpi=300)
print(f"✅ Dashboard generated: {output_img}")
