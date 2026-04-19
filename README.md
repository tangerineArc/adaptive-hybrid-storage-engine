# Adaptive Hybrid Storage Engine

This project architects and implements an **Adaptive Dual-Indexing Hybrid Engine**, a routing layer that dynamically combines the write speeds of Log-Structured Merge (LSM) Trees (via RocksDB) and the read speeds of B+Trees (via LMDB). By continuously monitoring the workload through a stochastic model, the engine can adapt its underlying storage structures on the fly. 

## Why?

The primary goal of this project is to overcome the inherent structural trade-offs between read-optimized and write-optimized storage engines. 
- B+Trees offer excellent point-read and range-scan performance but suffer from write amplification and fragmentation during heavy random writes.
- LSM-Trees handle massive write throughput efficiently by buffering and sequentially flushing to disk, but they incur a read penalty (read amplification) due to checking multiple levels and SSTables during scans.

By creating an intelligent routing layer, we aim to dynamically adapt to the workload, providing LSM-like write speeds during write-heavy phases and B+Tree-like read speeds during read-heavy phases, all while abstracting the complexity from the end-user.

## Background

### LSM-Trees (Log-Structured Merge-Trees)
Optimized for high-throughput writes. Data is first written to an in-memory MemTable and a Write-Ahead Log (WAL). Once the MemTable is full, it is flushed to disk as an immutable Sorted String Table (SSTable). This sequential writing pattern is highly efficient but requires compaction to clean up stale data and multiple disk reads to find a key.

### B+Trees
Optimized for fast reads and predictable latency. Data is maintained in a balanced tree structure on disk. Because the tree is constantly kept in sorted order, point reads and range scans are extremely fast. However, random writes cause page splits and random disk I/O, slowing down write-heavy operations.

### Adaptive Hybrid Access (AHA) Trees
An approach that divides the key space into smaller segments. Initially, segments are stored in a "cold" LSM format. As the system identifies "hot" segments that are frequently read, it morphs them into a "hot" B-Tree (often an in-memory B+Tree or similar structure) to speed up access. 

## Dev Setup and Usage

### Dependencies
The following installation instructions are for Fedora GNU/Linux distributions. For other OSs, use a package manager to find the equivalent dependencies.

```sh
sudo dnf install rocksdb-devel lmdb-devel abseil-cpp-devel cmake gcc g++
```

### Compilation
Navigate to the project's root directory in the terminal and execute the standard out-of-source CMake build process:

```sh
mkdir build
cd build
rm -rf * 
cmake -DCMAKE_BUILD_TYPE=Release .. 
make
```

### Running the Benchmark
Run the compiled executable with the desired number of keys and output CSV file:

```sh
./stress_test 1000000 telemetry_1M.csv
```

### Visualizing Results
To generate performance dashboards from the telemetry CSV, run the included Python script:

```sh
# Requires matplotlib, pandas, and seaborn
python ../plot.py telemetry_1M.csv
```

## Architecture Design

The project is structured around an abstract `Engine` interface, implementing three distinct systems:

1. **RocksBaseline**: A wrapper around RocksDB acting as the control group. It is tuned with a 512MB block cache and standard options.
2. **AhaTree**: Implements a segmented adaptive architecture. It splits keys into segments of 1000. A background thread monitors the read/write ratio per segment. If a segment's read ratio exceeds 70% and the expected gain outweighs the transformation cost, it "morphs" the segment from RocksDB into a hot in-memory `absl::btree_map`.
3. **HybridEngine**: The core dual-index adaptive router. 
   - **Write-Heavy State**: Uses only RocksDB for high-throughput writes.
   - **The "Brain"**: A background thread tracking a sliding window of the read/write ratio.
   - **Migration**: If the workload shifts to >80% reads, a migration is triggered. The engine takes a RocksDB snapshot and builds an LMDB instance in the background. Once built, new writes are sent to both databases (dual-write), but all reads are routed to the significantly faster LMDB.
   - **Reversion**: If the workload shifts back to write-heavy (<40% reads), dual-writing becomes a bottleneck. The engine drops the LMDB index and routes everything back to RocksDB.

A dedicated `Telemetry` class runs in the background, sampling latencies, throughput (Ops/Sec), RAM usage, and discrete system events (migrations, morphs) every 100ms.

## Results

To evaluate its performance, we benchmarked the Hybrid Engine against a raw RocksDB baseline and a custom implementation of an AHA (Adaptive Hybrid Access) tree.

We ran benchmarks across varying data sizes (1M, 3M, 5M, 8M, and 10M keys). The stress test simulates a dynamic workload over 120 seconds:
- **0s - 40s**: Write-heavy phase.
- **40s - 80s**: Read-heavy phase (Range Scans).
- **80s - 120s**: Write-heavy phase.

### Performance Summary (10M Keys)

| Engine    | Total Time (s) | Avg Scan (ms) | Peak P99 Scan | Avg Write (ms) | Peak RAM (MB) |
|-----------|----------------|---------------|---------------|----------------|---------------|
| AHA       | 120.0          | 1.2850        | 2.8720        | 0.0065         | 750.5         |
| HYBRID    | 120.0          | 0.0340        | 2.7840        | 0.0075         | 751.7         |
| ROCKSDB   | 120.0          | 1.0032        | 3.5740        | 0.0066         | 150.2         |

### Observations

1. **Scan Latency**: The Hybrid Engine achieves orders of magnitude faster average scan latencies. Once the LMDB index is active, range queries complete almost instantly compared to RocksDB's multi-level SSTable lookups.
2. **Throughput & Write Latency**: During write-heavy phases, the Hybrid Engine performs nearly identically to baseline RocksDB. Dual-writing during the read-heavy phase introduces a negligible write latency penalty (~0.001ms increase).
3. **Adaptability (The Events Timeline)**: The graphs demonstrate clear `MIGRATION_START` and `MIGRATION_END` events. During migration, scan latencies are momentarily high as they still hit RocksDB while the LMDB index builds. Post-migration, scan latency flatlines near zero. When the final write-heavy phase hits, the `REVERT` event fires, dropping LMDB and returning to baseline RocksDB metrics.
4. **Memory Footprint**: The primary trade-off of the Hybrid Engine (and AHA) is memory usage. Maintaining dual storage backends/hot-segment mappings spikes the RAM significantly compared to standard RocksDB. At 10M keys, Hybrid peaked at ~751 MB, while RocksDB stayed around 150 MB. A memory threshold failsafe is included in the Hybrid Engine to abort migration if RAM exceeds 90%.

## So?

The Adaptive Hybrid Engine effectively bridges the gap between B+Trees and LSM-Trees. By leveraging a stochastic background model, it successfully identifies read-heavy workloads, dynamically builds a secondary LMDB index, and routes queries to achieve blazing-fast reads. While the strategy introduces memory overhead and brief CPU spikes during index migration, the massive reduction in scan latency alongside the preservation of LSM write speeds proves it to be an incredibly powerful architecture for highly dynamic database environments.

As applications become increasingly complex, tuning a single storage engine to perfectly fit an unpredictable workload is nearly impossible. This dual-indexing routing layer abstracts this complexity, proving that we can programmatically identify workload shifts and swap underlying data structures to guarantee optimal performance without manual DBA intervention.

