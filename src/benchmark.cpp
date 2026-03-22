#include "AdaptiveRouter.h"

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <sstream>
#include <string>
#include <thread>

// Helper to format keys like "user:000001" to ensure they sort correctly
std::string MakeKey(int i) {
  std::ostringstream oss;
  oss << "user:" << std::setw(6) << std::setfill('0') << i;
  return oss.str();
}

int main() {
  const int NUM_RECORDS = 1'000'000;
  const int NUM_SCANS = 50'000;     // 50k range queries
  const int SCAN_RANGE = 50;        // Fetch 50 records per scan

  std::cout << "--- Starting benchmark ---\n";

  // ==========================================
  //  Ingestion Phase
  // ==========================================
  AdaptiveRouter router("./bench_storage");

  std::cout << "Ingesting " << NUM_RECORDS << " records into AdaptiveRouter...\n";
  for (int i = 0; i < NUM_RECORDS; i++) {
    router.Put(MakeKey(i), "data_payload_" + std::to_string(i));
  }

  // Wait for the background worker to finish tailing the WAL and building LMDB
  std::cout << "Waiting for asynchronous index to catch up...\n";
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // ==========================================
  //  Benchmarking Raw RocksDB (The Baseline)
  // ==========================================
  std::cout << "\nRunning " << NUM_SCANS << " scans on Raw RocksDB...\n";

  // We open a direct connection to a raw rocksdb for the baseline
  rocksdb::DB* raw_db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB::Open(options, "./raw_rocksdb_bench", &raw_db);

  // Pre-fill raw RocksDB
  for (int i = 0; i < NUM_RECORDS; ++i) {
    raw_db->Put(rocksdb::WriteOptions(), MakeKey(i), "data_payload_" + std::to_string(i));
  }

  auto start_rocks = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_SCANS; ++i) {
    // Pick a random starting point
    int start_idx = rand() % (NUM_RECORDS - SCAN_RANGE);
    std::string start_key = MakeKey(start_idx);
    std::string end_key = MakeKey(start_idx + SCAN_RANGE);

    rocksdb::Iterator* it = raw_db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key; it->Next()) {
      // Simulate reading the data
      volatile size_t s = it->value().size();
    }
    delete it;
  }

  auto end_rocks = std::chrono::high_resolution_clock::now();
  auto rocks_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_rocks - start_rocks).count();

  // ==========================================
  //  Benchmarking AdaptiveRouter
  // ==========================================
  std::cout << "Running " << NUM_SCANS << " scans on AdaptiveRouter (LMDB B+Tree)...\n";

  auto start_router = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_SCANS; ++i) {
    int start_idx = rand() % (NUM_RECORDS - SCAN_RANGE);
    std::string start_key = MakeKey(start_idx);
    std::string end_key = MakeKey(start_idx + SCAN_RANGE);

    auto results = router.Scan(start_key, end_key);
    // Data is already fetched in the results vector
    volatile size_t s = results.size();
  }

  auto end_router = std::chrono::high_resolution_clock::now();
  auto router_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_router - start_router).count();

  // ==========================================
  //  Results
  // ==========================================
  std::cout << "\n=== RESULTS ===\n";
  std::cout << "Raw RocksDB Time:     " << rocks_duration << " ms\n";
  std::cout << "AdaptiveRouter Time:  " << router_duration << " ms\n";

  if (router_duration < rocks_duration) {
    double speedup = (double)rocks_duration / router_duration;
    std::cout << "WIN! AdaptiveRouter is " << std::setprecision(2) << speedup << "x faster!\n";
  }

  delete raw_db;
  return 0;
}
