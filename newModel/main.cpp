#include "AHATree.h"
#include <iostream>
#include <string>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <thread>

using namespace std;
using namespace std::chrono;

string format_key(int i) {
    stringstream ss;
    ss << "key_" << setw(7) << setfill('0') << i;
    return ss.str();
}

int main() {
    cout << "========================================================\n";
    cout << "🌲 AHA-TREE PROTOTYPE: SPRINT 4 (Dual-Path & Locking)\n";
    cout << "========================================================\n\n";

    std::filesystem::remove_all("aha_rocks_data");
    AHATree db(5000);

    cout << "\n[PHASE 1] Seeding 5000 keys into Cold LSM...\n";
    for (int i = 0; i < 5000; i++) {
        db.Put(format_key(i), "baseline_payload");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2500)); 

    cout << "\n[PHASE 2] Simulating SKEWED Analytical Workload...\n";
    for (int i = 0; i < 600; i++) {
        db.Scan(format_key(2010), format_key(2050));
        std::this_thread::sleep_for(std::chrono::milliseconds(2)); 
    }

    cout << "[TEST] Waiting for Monitor to trigger Morphing...\n";
    std::this_thread::sleep_for(std::chrono::seconds(3)); 

    cout << "\n[PHASE 3] Live Consistency Guard Test...\n";
    cout << "Firing 100 new LIVE WRITES into Segment [2]...\n";
    for (int i = 2000; i < 2100; i++) {
        db.Put(format_key(i), "UPDATED_LIVE_PAYLOAD");
    }

    cout << "Verifying Keyspace State...\n";
    db.DebugPrintSegmentState(1); // COLD
    db.DebugPrintSegmentState(2); // HOT

    cout << "\n[PHASE 4] The Payoff: Dual-Path Speed Test...\n";
    
    // Test the COLD Segment (RocksDB)
    auto start_cold = high_resolution_clock::now();
    for (int i = 0; i < 1000; i++) db.Scan(format_key(1000), format_key(1999));
    auto end_cold = high_resolution_clock::now();
    double cold_ms = duration_cast<milliseconds>(end_cold - start_cold).count();

    // Test the HOT Segment (std::map B+ Tree)
    auto start_hot = high_resolution_clock::now();
    for (int i = 0; i < 1000; i++) db.Scan(format_key(2000), format_key(2999));
    auto end_hot = high_resolution_clock::now();
    double hot_ms = duration_cast<milliseconds>(end_hot - start_hot).count();

    cout << "❄️ 1000 Scans on COLD Segment [1]: " << cold_ms << " ms\n";
    cout << "🔥 1000 Scans on HOT Segment [2]:  " << hot_ms << " ms\n";
    
    if (hot_ms > 0) cout << "🚀 Performance Gain: " << (cold_ms / hot_ms) << "x Faster!\n";

    cout << "\n========================================================\n";
    cout << "✅ SPRINT 4 COMPLETE.\n";
    cout << "========================================================\n";

    return 0;
}
