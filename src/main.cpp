#include "engines/AHATree.h"
#include "engines/HybridEngine.h"
#include "engines/RocksBaseline.h"
#include "utils/Telemetry.h"

#include <chrono>
#include <iostream>
#include <random>

using namespace std;
using namespace std::chrono;

const uint64_t TOTAL_KEYS = 1'000'000;

// Big-Endian binary encoding for sequential disk sorting
string encode_timestamp(uint64_t ts) {
  uint64_t swapped = __builtin_bswap64(ts);
  return string(reinterpret_cast<const char*>(&swapped), sizeof(swapped));
}

void run_engine_stress_test(Engine* db, Telemetry& tracker) {
  cout << "\n[==================================================]\n";
  cout << "🚀 INITIATING TEST: " << db->GetName() << "\n";
  cout << "[==================================================]\n";

  // Pre-allocate 4KB (4096 bytes) dummy payloads ONCE.
  std::string heavy_payload(4096, 'x');
  // std::string heavy_payload = "my_payload_123";
  std::string update_payload(4096, 'y');
  // std::string update_payload = "update_payload_123";

  cout << "[WARM-UP] Pumping " << TOTAL_KEYS << " keys to populate data structures...\n";
  tracker.set_context( db->GetName(), db->GetActiveBackend(), "WRITE");

  auto start_warmup = high_resolution_clock::now();
  for(uint64_t i = 0; i < TOTAL_KEYS; i++) {
    db->Put(encode_timestamp(i), heavy_payload);
  }
  auto end_warmup = high_resolution_clock::now();
  cout << "Warm-up complete in " << duration_cast<milliseconds>(end_warmup - start_warmup).count() << " ms.\n";

  cout << "\n[STRESS] Entering 120-second dynamic workload simulation...\n";
  auto test_start = steady_clock::now();

  mt19937 rng(42);
  uniform_int_distribution<uint64_t> hot_key_dist(50000, 59000);

  int phase = 1;
  while(duration_cast<seconds>(steady_clock::now() - test_start).count() < 120) {
    auto current_sec = duration_cast<seconds>(steady_clock::now() - test_start).count();

    if (current_sec >= 40 && phase == 1) {
      cout << "\n>>> SHIFT: Triggering Read-Heavy phase... (Testing Index Routing)\n";
      phase = 2;
    } else if (current_sec >= 80 && phase == 2) {
      cout << "\n>>> SHIFT: Triggering Write-Heavy phase... (Testing Degradation/Reverts)\n";
      phase = 3;
    }

    uint64_t k = hot_key_dist(rng);
    auto op_start = high_resolution_clock::now();

    if (phase == 1 || phase == 3) {
      tracker.set_context(db->GetName(), db->GetActiveBackend(), "WRITE");
      db->Put(encode_timestamp(k), update_payload);

      auto op_end = high_resolution_clock::now();
      double latency = duration_cast<microseconds>(op_end - op_start).count() / 1000.0;
      tracker.log_op(latency, false);
    } else {
      tracker.set_context(db->GetName(), db->GetActiveBackend(), "SCAN");
      db->Scan(encode_timestamp(k), encode_timestamp(k + 1000));
      auto op_end = high_resolution_clock::now();
      double latency = duration_cast<microseconds>(op_end - op_start).count() / 1000.0;
      tracker.log_op(latency, true);
    }
  }

  auto test_end = steady_clock::now();
  double total_ms = duration_cast<milliseconds>(test_end - test_start).count();
  tracker.finalize_engine(db->GetName(), total_ms);
  cout << "✅ " << db->GetName() << " Test Complete.\n";
}

int main() {
  cout << "Conditions: 1M Keys Warmup, 120s Mutating Workload. Big-Endian Mode.\n";
  Telemetry tracker;

  {
    RocksBaseline baseline;
    run_engine_stress_test(&baseline, tracker);
  }
  {
    HybridEngine hybrid(tracker);
    run_engine_stress_test(&hybrid, tracker);
  }
  {
    AhaTree aha(TOTAL_KEYS, tracker);
    run_engine_stress_test(&aha, tracker);
  }

  tracker.print_summary_table();
  cout << "\n✅ CSV export complete. 'telemetry.csv' is ready.\n";
  return 0;
}
