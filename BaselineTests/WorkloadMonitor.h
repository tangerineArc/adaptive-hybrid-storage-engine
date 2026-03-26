#pragma once

#include <atomic>
#include <thread>
#include <deque>
#include <vector>
#include <chrono>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <mutex>

enum class WorkloadState { STABLE, READ_HEAVY, WRITE_HEAVY };

class WorkloadMonitor {
private:
    // 1. Atomic Counters (Zero overhead for the database threads)
    std::atomic<uint64_t> read_ops{0};
    std::atomic<uint64_t> write_ops{0};

    // 2. Sliding Window & State
    std::deque<double> history;
    WorkloadState current_state{WorkloadState::STABLE};
    std::mutex state_mutex; // To safely read state from main thread

    // Constants from Sprint Specification
    const double T_HIGH = 0.8;
    const double T_LOW = 0.4;
    const int W = 5; // Window size
    const int K = 3; // 3-out-of-5 rule

    // 3. Thread Management
    std::atomic<bool> running{true};
    std::thread background_thread;
    int window_ms;

    // 4. Sprint 1 Cost Metrics (Microseconds)
    const double OMEGA_LSM = 0.17; 
    const double ALPHA = 0.062;    

    // --- Helper Guards ---
    bool check_ram_guard() {
        // In a full implementation, you'd check OS memory here.
        // For now, we assume we have enough RAM.
        return true; 
    }

    bool check_cost_gate(double current_lsm_latency) {
        // Only migrate if current LSM reads are slower than building the index
        // Placeholder logic for Q* calculation
        return current_lsm_latency > ALPHA; 
    }

    void log_trace(uint64_t r, uint64_t w, double ratio, const std::string& state_str, bool triggered) {
        std::ofstream file("workload_trace.csv", std::ios::app);
        if (file.is_open()) {
            auto now = std::chrono::system_clock::now().time_since_epoch().count();
            file << now << "," << r << "," << w << "," << ratio << "," << state_str << "," << (triggered ? "YES" : "NO") << "\n";
        }
    }

    // --- The Background Loop ---
    void observer_loop() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(window_ms));
            if (!running) break;

            // 1. SNAP THE COUNTERS
            uint64_t current_reads = read_ops.exchange(0);
            uint64_t current_writes = write_ops.exchange(0);
            uint64_t total = current_reads + current_writes;

            double ratio = 0.0;
            if (total > 0) {
                ratio = static_cast<double>(current_reads) / total;
            }

            // 2. UPDATE SLIDING WINDOW
            history.push_front(ratio);
            if (history.size() > W) {
                history.pop_back();
            }

            // 3. EVALUATE HYSTERESIS MATH
            int high_count = 0;
            for (double r : history) {
                if (r >= T_HIGH) high_count++;
            }

            std::string state_str = "STABLE";
            bool migration_triggered = false;

            std::lock_guard<std::mutex> lock(state_mutex);
            
            // State Machine Transitions
            if (current_state != WorkloadState::READ_HEAVY && high_count >= K) {
                current_state = WorkloadState::READ_HEAVY;
                state_str = "READ_HEAVY";
                
                // Fire the Guards!
                if (check_ram_guard() && check_cost_gate(0.1)) {
                    migration_triggered = true;
                    std::cout << "\n[BRAIN]  MIGRATION TRIGGERED! Workload is Read-Heavy.\n";
                }
            } 
            else if (current_state == WorkloadState::READ_HEAVY && ratio < T_LOW) {
                // Notice we look at the IMMEDIATE ratio for T_LOW to drop out quickly to save RAM
                current_state = WorkloadState::WRITE_HEAVY;
                state_str = "WRITE_HEAVY";
                std::cout << "\n[BRAIN]  REVERTING. Workload became Write-Heavy. Dropping Index.\n";
                history.assign(W, 0.0);
            } 
            else if (current_state == WorkloadState::STABLE && ratio < T_LOW && total > 0) {
                 current_state = WorkloadState::WRITE_HEAVY;
                 state_str = "WRITE_HEAVY";
            }
            else {
                // Maintain current state string for logging
                if (current_state == WorkloadState::READ_HEAVY) state_str = "READ_HEAVY";
                if (current_state == WorkloadState::WRITE_HEAVY) state_str = "WRITE_HEAVY";
            }

            log_trace(current_reads, current_writes, ratio, state_str, migration_triggered);
        }
    }

public:
    WorkloadMonitor(int window_duration_ms = 10000) : window_ms(window_duration_ms) {
        // Initialize log file header
        std::ofstream file("workload_trace.csv");
        file << "Timestamp,Reads,Writes,ReadRatio,State,MigrationTriggered\n";
        
        // Boot up the Brain thread
        background_thread = std::thread(&WorkloadMonitor::observer_loop, this);
    }

    ~WorkloadMonitor() {
        running = false;
        if (background_thread.joinable()) {
            background_thread.join();
        }
    }

    // Zero-overhead atomic hooks for your StorageManager
    inline void record_read() { read_ops.fetch_add(1, std::memory_order_relaxed); }
    inline void record_write() { write_ops.fetch_add(1, std::memory_order_relaxed); }

    WorkloadState get_state() {
        std::lock_guard<std::mutex> lock(state_mutex);
        return current_state;
    }
};
