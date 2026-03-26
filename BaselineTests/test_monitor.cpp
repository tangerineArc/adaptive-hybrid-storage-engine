#include "WorkloadMonitor.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace std;
using namespace std::chrono;

void simulate_traffic(WorkloadMonitor& monitor, int duration_ms, double read_probability, const string& phase_name) {
    cout << "\n--- Starting Phase: " << phase_name << " ---" << endl;
    cout << "Target Read Ratio: " << (read_probability * 100) << "%" << endl;
    
    auto start = high_resolution_clock::now();
    while (duration_cast<milliseconds>(high_resolution_clock::now() - start).count() < duration_ms) {
        // Simulate aggressive queries
        for (int i = 0; i < 1000; i++) {
            if ((rand() % 100) < (read_probability * 100)) {
                monitor.record_read();
            } else {
                monitor.record_write();
            }
        }
        std::this_thread::sleep_for(milliseconds(10)); // Prevent completely locking CPU in test
    }
}

int main() {
    srand(42);
    
    // Create monitor with a 500ms window so our test runs fast
    cout << "Booting up Workload Monitor (Brain Thread)..." << endl;
    WorkloadMonitor monitor(500); 

    // Phase 1: Heavy Writes (Like an initial data ingestion)
    // 90% writes, 10% reads. Duration: 2000ms (4 windows)
    simulate_traffic(monitor, 2000, 0.10, "Data Ingestion (Write Heavy)");

    // Phase 2: THE JITTER TEST (Signal-to-Noise)
    // Sudden 95% reads, but only for 600ms (roughly 1 window). 
    // The Brain SHOULD IGNORE THIS because of the 3-out-of-5 rule!
    simulate_traffic(monitor, 600, 0.95, "Dashboard Refresh Spike (Noise)");

    // Phase 3: Return to Normal Writes
    simulate_traffic(monitor, 1500, 0.20, "Return to Writes");

    // Phase 4: Sustained Read Workload
    // 90% reads for 2500ms (5 windows). 
    // The Brain MUST TRIGGER MIGRATION here.
    simulate_traffic(monitor, 2500, 0.90, "Analytical Queries (Read Heavy)");

    // Phase 5: Revert Threshold Test (Hysteresis)
    // 100% writes for 1000ms. 
    // The Brain MUST REVERT state because ratio drops below T_LOW (0.4).
    simulate_traffic(monitor, 1000, 0.00, "Sudden Ingestion (Revert)");

    cout << "\nTest Complete. Check workload_trace.csv for the exact timeline." << endl;
    return 0;
}
