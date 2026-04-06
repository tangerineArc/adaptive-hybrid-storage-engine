#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono;

struct EngineStats {
  double total_time_ms = 0;

  // Averages
  double avg_scan_latency = 0;
  double avg_write_latency = 0;

  // Worst-case P99 tracking
  double max_p99_scan = 0;
  double max_p99_write = 0;

  double peak_ram_mb = 0;
  uint64_t total_scans = 0;
  uint64_t total_writes = 0;
};

class Telemetry {
private:
  std::ofstream log_file;
  std::atomic<bool> running{true};
  std::thread bg_thread;

  // Aggregation buckets
  std::atomic<uint64_t> interval_ops{0};
  std::atomic<double> interval_latency_sum{0};
  std::vector<double> interval_latencies;

  time_point<steady_clock> start_time;
  std::mutex write_lock;

  std::string current_system = "NONE";  // For final summary aggregation (e.g. "HYBRID")
  std::string current_backend = "NONE"; // For CSV logging (e.g. "LMDB")
  std::string current_op_type = "IDLE";

  std::map<std::string, EngineStats> final_stats;
  double current_peak_ram = 0;

  double get_rss_mb() {
    std::ifstream statm("/proc/self/statm");
    if (!statm) return 0.0;
    long dummy, rss;
    statm >> dummy >> rss;
    return (rss * sysconf(_SC_PAGESIZE)) / (1024.0 * 1024.0);
  }

public:
  Telemetry() {
    log_file.open("telemetry.csv");
    log_file << "Timestamp,Engine,OpType,AvgLatency_ms,P99Latency_ms,OpsPerSec,RamMB,Event\n";
    start_time = steady_clock::now();

    // Background Aggregator Thread (Sprint 5.6: Runs every 100ms)
    bg_thread = std::thread([this]() {
      while(running) {
        std::this_thread::sleep_for(milliseconds(100));
        if (!running) break;

        uint64_t ops = interval_ops.exchange(0);
        double lat_sum = interval_latency_sum.exchange(0);
        double ram = get_rss_mb();

        if (ram > current_peak_ram) current_peak_ram = ram;

        double avg_lat = ops > 0 ? (lat_sum / ops) : 0;
        double p99_lat = 0.0;

        std::lock_guard<std::mutex> lock(write_lock);

        if (!interval_latencies.empty()) {
          std::sort(interval_latencies.begin(), interval_latencies.end());
          int p99_index = static_cast<int>(interval_latencies.size() * 0.99);
          if (p99_index >= interval_latencies.size()) p99_index = interval_latencies.size() - 1;
          p99_lat = interval_latencies[p99_index];

          auto& stats = final_stats[current_system];
          if (current_op_type == "SCAN" && p99_lat > stats.max_p99_scan) stats.max_p99_scan = p99_lat;
          if (current_op_type == "WRITE" && p99_lat > stats.max_p99_write) stats.max_p99_write = p99_lat;

          interval_latencies.clear();
        }

        // Multiply by 10 because interval is 100ms
        double ops_sec = ops * 10.0;

        auto now = steady_clock::now();
        double ts = duration_cast<milliseconds>(now - start_time).count() / 1000.0;

        // Use current_backend for the CSV so it tracks the shifting LMDB/RocksDB states
        log_file << ts << "," << current_backend << "," << current_op_type << ","
                 << avg_lat << "," << p99_lat << "," << ops_sec << "," << ram << ",NONE\n";
        log_file.flush();
      }
    });
  }

  ~Telemetry() {
    running = false;
    if(bg_thread.joinable()) bg_thread.join();
    log_file.close();
  }

  // Updated to track both the parent system and the active storage engine
  void set_context(const std::string& system, const std::string& backend, const std::string& op_type) {
    current_system = system;
    current_backend = backend;
    current_op_type = op_type;
  }

  void log_op(double latency_ms, bool is_scan) {
    interval_ops.fetch_add(1, std::memory_order_relaxed);

    double current_sum = interval_latency_sum.load();
    while(!interval_latency_sum.compare_exchange_weak(current_sum, current_sum + latency_ms));

    std::lock_guard<std::mutex> lock(write_lock);
    interval_latencies.push_back(latency_ms);

    // Aggregate to the System level (e.g. "HYBRID") so the final table doesn't break
    auto& stats = final_stats[current_system];
    if (is_scan) {
      stats.avg_scan_latency += latency_ms;
      stats.total_scans++;
    } else {
      stats.avg_write_latency += latency_ms;
      stats.total_writes++;
    }
  }

  void log_discrete_event(const std::string& system, const std::string& event) {
    std::lock_guard<std::mutex> lock(write_lock);
    double ts = duration_cast<milliseconds>(steady_clock::now() - start_time).count() / 1000.0;
    log_file << ts << "," << system << ",SYSTEM,0,0,0," << get_rss_mb() << "," << event << "\n";
  }

  void finalize_engine(const std::string& engine, double total_time_ms) {
      auto& stats = final_stats[engine];
      stats.total_time_ms = total_time_ms;
      stats.peak_ram_mb = current_peak_ram;
      if (stats.total_scans > 0) stats.avg_scan_latency /= stats.total_scans;
      if (stats.total_writes > 0) stats.avg_write_latency /= stats.total_writes;
      current_peak_ram = 0;
  }

  void print_summary_table() {
    std::cout << "\n==============================================================================================================\n";
    std::cout << "🏆 FINAL BENCHMARK RESULTS\n";
    std::cout << "==============================================================================================================\n";
    std::cout << std::left << std::setw(10) << "Engine"
              << std::setw(18) << "Total Time (s)"
              << std::setw(18) << "Avg Scan (ms)"
              << std::setw(18) << "Peak P99 Scan"
              << std::setw(18) << "Avg Write (ms)"
              << std::setw(18) << "Peak RAM (MB)" << "\n";
    std::cout << "--------------------------------------------------------------------------------------------------------------\n";

    for (const auto& [name, stats] : final_stats) {
      if (name == "NONE") continue;
      std::cout << std::left << std::setw(10) << name
                << std::setw(18) << std::fixed << std::setprecision(1)  << stats.total_time_ms / 1000.0
                << std::setw(18) << std::fixed << std::setprecision(4) << stats.avg_scan_latency
                << std::setw(18) << stats.max_p99_scan
                << std::setw(18) << stats.avg_write_latency
                << std::setw(18) << std::setprecision(1) << stats.peak_ram_mb << "\n";
    }
    std::cout << "==============================================================================================================\n";
  }

  double current_ram_percent() {
    std::ifstream meminfo("/proc/meminfo");
    long total = 1, available = 1;
    std::string line;
    while(std::getline(meminfo, line)) {
      if (line.find("MemTotal:") == 0) sscanf(line.c_str(), "%*s %ld", &total);
      if (line.find("MemAvailable:") == 0) sscanf(line.c_str(), "%*s %ld", &available);
    }
    return 100.0 * (1.0 - (double)available / total);
  }
};
