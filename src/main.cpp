#include "engines/AHATree.h"
#include "engines/HybridEngine.h"
#include "engines/RocksBaseline.h"
#include "utils/Telemetry.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>

using namespace std;
using namespace std::chrono;

// Big-Endian binary encoding for sequential disk sorting
string encode_timestamp(uint64_t ts) {
  uint64_t swapped = __builtin_bswap64(ts);
  return string(reinterpret_cast<const char*>(&swapped), sizeof(swapped));
}

void run_engine_stress_test(Engine* db, Telemetry& tracker, uint64_t total_keys) {
  cout << "\n[==================================================]\n";
  cout << "🚀 INITIATING TEST: " << db->GetName() << " (" << total_keys << " keys)\n";
  cout << "[==================================================]\n";

  std::string heavy_payload(128, 'x');
  // std::string heavy_payload = "my_payload_123";
  std::string update_payload(128, 'y');
  // std::string update_payload = "update_payload_123";

  cout << "[WARM-UP] Pumping " << total_keys << " keys to populate data structures...\n";
  tracker.set_context( db->GetName(), db->GetActiveBackend(), "WRITE");

  auto start_warmup = high_resolution_clock::now();
  for(uint64_t i = 0; i < total_keys; i++) {
    db->Put(encode_timestamp(i), heavy_payload);
  }
  auto end_warmup = high_resolution_clock::now();
  cout << "Warm-up complete in " << duration_cast<milliseconds>(end_warmup - start_warmup).count() << " ms.\n";

  cout << "\n[STRESS] Entering 120-second dynamic workload simulation...\n";
  auto test_start = steady_clock::now();

  mt19937 rng(42);
  uniform_int_distribution<uint64_t> hot_key_dist(total_keys * 0.05, total_keys * 0.06);

  int phase = 1;
  while(duration_cast<seconds>(steady_clock::now() - test_start).count() < 120) {
    auto current_sec = duration_cast<seconds>(steady_clock::now() - test_start).count();

    if (current_sec >= 40 && phase == 1) {
      cout << "\n>>> SHIFT: Triggering Read-Heavy phase...\n";
      phase = 2;
    } else if (current_sec >= 80 && phase == 2) {
      cout << "\n>>> SHIFT: Triggering Write-Heavy phase...\n";
      phase = 3;
    }

    uint64_t k = hot_key_dist(rng);
    auto op_start = high_resolution_clock::now();

    if (phase == 1 || phase == 3) {
      tracker.set_context(db->GetName(), db->GetActiveBackend(), "WRITE");
      db->Put(encode_timestamp(k), update_payload);

      auto op_end = high_resolution_clock::now();
      tracker.log_op(duration_cast<microseconds>(op_end - op_start).count() / 1000.0, false);
    } else {
      tracker.set_context(db->GetName(), db->GetActiveBackend(), "SCAN");
      db->Scan(encode_timestamp(k), encode_timestamp(k + 1000));

      auto op_end = high_resolution_clock::now();
      tracker.log_op(duration_cast<microseconds>(op_end - op_start).count() / 1000.0, true);
    }
  }

  double total_ms = duration_cast<milliseconds>(steady_clock::now() - test_start).count();
  tracker.finalize_engine(db->GetName(), total_ms);
  cout << "✅ " << db->GetName() << " Test Complete.\n";
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    cerr << "Usage: " << argv[0] << " <total_keys> <ouput_csv>\n";
    return 1;
  }

  uint64_t total_keys = stoull(argv[1]);
  string csv_filename = argv[2];

  cout << "Conditions: " << total_keys << " Keys Warmup, 120s Mutating Workload.\n";
  Telemetry tracker(csv_filename);

  {
    RocksBaseline baseline;
    run_engine_stress_test(&baseline, tracker, total_keys);
  }
  {
    HybridEngine hybrid(tracker, total_keys, 128);
    run_engine_stress_test(&hybrid, tracker, total_keys);
  }
  {
    AhaTree aha(total_keys, tracker);
    run_engine_stress_test(&aha, tracker, total_keys);
  }

  tracker.print_summary_table();
  cout << "\n✅ Data exported to " << csv_filename << "\n";
  return 0;
}
