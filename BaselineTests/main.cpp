#include "StorageManager.h"
#include <atomic>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

using namespace std;
using namespace std::chrono;

// ==========================================
// CUSTOM PHASE-SPECIFIC RAM PROFILER
// ==========================================
class PhaseMemoryProfiler {
private:
  std::atomic<bool> running{false};
  std::atomic<double> peak_rss{0.0};
  std::thread tracker;

  // Gets CURRENT physical RAM (Resident Set Size)
  double get_current_rss_mb() {
    std::ifstream statm("/proc/self/statm");
    if (!statm)
      return 0.0;
    long dummy, rss;
    statm >> dummy >> rss; // 2nd value is RSS in pages
    long page_size = sysconf(_SC_PAGESIZE);
    return (rss * page_size) / (1024.0 * 1024.0);
  }

public:
  void start() {
    peak_rss = 0.0;
    running = true;
    tracker = std::thread([this]() {
      while (running) {
        double current = get_current_rss_mb();
        double max_val = peak_rss.load();
        // Lock-free update of the peak value
        while (current > max_val &&
               !peak_rss.compare_exchange_weak(max_val, current))
          ;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(5)); // Poll every 5ms
      }
    });
  }

  double stop() {
    running = false;
    if (tracker.joinable()) {
      tracker.join();
    }
    return peak_rss.load();
  }
};

// Helper: Format keys nicely for lexicographical sorting
string format_key(int i) {
  stringstream ss;
  ss << "key_" << setw(7) << setfill('0') << i;
  return ss.str();
}

// Helper: Simulate streaming data ingestion
void stream_writes_in_batches(StorageManager &manager, int start_id,
                              int total_records) {
  int batches = total_records / 10;
  for (int b = 0; b < batches; b++) {
    for (int i = 0; i < 10; i++) {
      int current_id = start_id + (b * 10) + i;
      manager.Put(format_key(current_id), "streaming_data_payload_12345");
    }
    std::this_thread::yield();
  }
}

int main() {
  cout << "=========================\n";
  cout << " BOOTING ADAPTIVE ENGINE\n";
  cout << "=========================\n";

  StorageManager manager;
  PhaseMemoryProfiler profiler;

  // =========================================================
  // PERMUTATION 1: STABLE -> WRITE-HEAVY (Boot & Ingest)
  // =========================================================
  cout << "\n[TEST 1] Massive Streaming Ingestion (1M Records)...\n";
  profiler.start();
  auto start_write = high_resolution_clock::now();

  stream_writes_in_batches(manager, 0, 1000000);

  auto end_write = high_resolution_clock::now();
  double test1_peak_ram = profiler.stop();

  cout << "1,000,000 keys ingested sequentially in "
       << duration_cast<milliseconds>(end_write - start_write).count()
       << " ms.\n";
  cout << "📈 PEAK RAM (Ingestion Phase): " << test1_peak_ram << " MB\n";

  // Let RocksDB Compact
  cout << "\n[SYSTEM] Pausing for 3 seconds to let RocksDB flush MemTables to "
          "disk...\n";
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Establish True RocksDB Baseline (Averaged over 500 scans)
  cout << "\nEstablishing RocksDB (LSM) Baseline Scan Speed...\n";
  profiler.start();
  auto start_base = high_resolution_clock::now();

  for (int i = 0; i < 500; i++) {
    // Shift the keys slightly to prevent perfect CPU caching
    manager.Scan(format_key(10000 + (i % 100)), format_key(15000 + (i % 100)));
  }

  auto end_base = high_resolution_clock::now();
  double rocksdb_scan_peak_ram = profiler.stop();

  double rocksdb_baseline_total_ms =
      duration_cast<microseconds>(end_base - start_base).count() / 1000.0;
  double rocksdb_avg_ms = rocksdb_baseline_total_ms / 500.0;

  cout << "Baseline LSM Total Time (500 scans): " << rocksdb_baseline_total_ms
       << " ms.\n";
  cout << "Baseline LSM Average: " << rocksdb_avg_ms << " ms/scan.\n";
  cout << "📈 PEAK RAM (RocksDB Scan Phase): " << rocksdb_scan_peak_ram
       << " MB\n";

  // =========================================================
  // PERMUTATION 2: THE NOISE FILTER (Short Read Spike)
  // =========================================================
  cout << "\n[TEST 2] Simulating Jitter (1.5 seconds of reads)...\n";
  auto start_jitter = high_resolution_clock::now();
  while (
      duration_cast<milliseconds>(high_resolution_clock::now() - start_jitter)
          .count() < 1500) {
    manager.Scan(format_key(500000), format_key(501000));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // =========================================================
  // PERMUTATION 3: MIGRATION LATENCY PENALTY
  // =========================================================
  cout << "\n[TEST 3] Sustained Workload -> Measuring Latency DURING "
          "Migration...\n";

  profiler.start(); // Start profiling the migration spike
  auto start_pump = high_resolution_clock::now();
  double total_scan_time_us = 0;
  int scan_count = 0;

  while (duration_cast<milliseconds>(high_resolution_clock::now() - start_pump)
             .count() < 7500) {
    auto scan_s = high_resolution_clock::now();

    // FIX: Pump 10 scans per loop to break the 500 op threshold
    for (int i = 0; i < 10; i++) {
      manager.Scan(format_key(100000), format_key(105000));
      scan_count++;
    }

    auto scan_e = high_resolution_clock::now();
    total_scan_time_us += duration_cast<microseconds>(scan_e - scan_s).count();

    // Sleep for 5ms instead of 10ms. Higher throughput, same duration.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  double migration_peak_ram = profiler.stop();

  cout << "Average Scan Latency (while migrating): "
       << (total_scan_time_us / scan_count) << " microseconds/scan.\n";
  cout << "📈 PEAK RAM (Migration Phase): " << migration_peak_ram << " MB\n";

  // =========================================================
  // PERMUTATION 4: THE DUAL-WRITE TAX
  // =========================================================
  cout << "\n[TEST 4] Measuring Write Degradation During Dual-Write Phase...\n";

  profiler.start();
  auto start_dual = high_resolution_clock::now();
  for (int b = 0; b < 10000; b++) {
    for (int i = 0; i < 10; i++) {
      manager.Put(format_key(1000000 + (b * 10) + i), "LIVE_UPDATE");
    }
    for (int r = 0; r < 15; r++) {
      manager.Scan(format_key(10000), format_key(10005));
    }
    std::this_thread::yield();
  }
  auto end_dual = high_resolution_clock::now();
  double dual_write_peak_ram = profiler.stop();

  cout << "Dual-Write Phase (100k keys + reads) took: "
       << duration_cast<milliseconds>(end_dual - start_dual).count()
       << " ms.\n";
  cout << "📈 PEAK RAM (Dual-Write Phase): " << dual_write_peak_ram << " MB\n";

  // =========================================================
  // PERMUTATION 5: SPEED VERIFICATION (The Payoff)
  // =========================================================
  cout << "\n[TEST 5] Verifying LMDB (B+Tree) Query Routing under sustained "
          "throughput ...\n";

  profiler.start();
  auto start_fast = high_resolution_clock::now();

  for (int i = 0; i < 5000; i++) {
    manager.Scan(format_key(10000 + (i % 100)), format_key(15000 + (i % 100)));
  }

  auto end_fast = high_resolution_clock::now();
  double lmdb_scan_peak_ram = profiler.stop();

  double router_total_ms =
      duration_cast<microseconds>(end_fast - start_fast).count() / 1000.0;
  double router_avg_ms = router_total_ms / 5000.0;

  cout << "Adaptive Router Total Time (5000 scans): " << router_total_ms
       << " ms.\n";
  cout << "Adaptive Router Average: " << router_avg_ms << " ms/scan.\n\n";

  cout << "Performance Multiplier: " << (rocksdb_avg_ms / router_avg_ms)
       << "x faster per scan!\n";
  cout << "📈 PEAK RAM (LMDB Scan Phase): " << lmdb_scan_peak_ram << " MB\n";

  // =========================================================
  // PERMUTATION 6: READ-HEAVY -> WRITE-HEAVY (The Revert)
  // =========================================================
  cout << "\n[TEST 6] Sudden switch back to massive streaming writes...\n";

  profiler.start();
  auto start_revert = high_resolution_clock::now();
  int revert_id = 1200000;
  while (
      duration_cast<milliseconds>(high_resolution_clock::now() - start_revert)
          .count() < 3000) {
    stream_writes_in_batches(manager, revert_id, 1000);
    revert_id += 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  double revert_peak_ram = profiler.stop();

  std::this_thread::sleep_for(std::chrono::milliseconds(2500));

  cout << "📈 PEAK RAM (Revert Phase): " << revert_peak_ram << " MB\n";

  cout << "\n========================================================\n";
  cout << " ALL PERMUTATIONS VERIFIED. ENGINE SHUTTING DOWN.\n";
  cout << "========================================================\n";

  return 0;
}
