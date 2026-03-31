#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <string>
#include <vector>
#include <map>
#include <atomic>
#include <shared_mutex>
#include <mutex> 
#include <iostream>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <thread>
#include <chrono>

enum class SegmentState {
    COLD_LSM,    
    HOT_BPLUS    
};

struct AdaptiveSegment {
    int segment_id;
    SegmentState state;
    
    std::atomic<uint64_t> read_count{0};
    std::atomic<uint64_t> write_count{0};
    
    std::shared_mutex rw_lock;
    std::map<std::string, std::string> hot_index;

    AdaptiveSegment(int id) : segment_id(id), state(SegmentState::COLD_LSM) {}
};

class AHATree {
private:
    rocksdb::DB* rocks_db;
    std::vector<std::unique_ptr<AdaptiveSegment>> segments;
    const int KEYS_PER_SEGMENT = 1000; 

    std::atomic<bool> running{true};
    std::thread monitor_thread;

    int get_segment_id(const std::string& key) {
        try {
            int key_numeric = std::stoi(key.substr(4));
            int seg_id = key_numeric / KEYS_PER_SEGMENT;
            if (seg_id >= segments.size()) return segments.size() - 1; 
            return seg_id;
        } catch (...) { return 0; }
    }

    std::string format_key_internal(int i) {
        std::stringstream ss;
        ss << "key_" << std::setw(7) << std::setfill('0') << i;
        return ss.str();
    }

    void Morph(int segment_id) {
        auto& seg = segments[segment_id];
        std::unique_lock<std::shared_mutex> lock(seg->rw_lock);

        if (seg->state == SegmentState::HOT_BPLUS) return;

        std::cout << "\n[MORPH] 🦋 Metamorphosis started for Segment [" << segment_id << "]...\n";

        std::string start_key = format_key_internal(segment_id * KEYS_PER_SEGMENT);
        std::string end_key = format_key_internal((segment_id + 1) * KEYS_PER_SEGMENT - 1);

        rocksdb::Iterator* it = rocks_db->NewIterator(rocksdb::ReadOptions());
        int count = 0;
        
        for (it->Seek(start_key); it->Valid() && it->key().compare(end_key) <= 0; it->Next()) {
            seg->hot_index[it->key().ToString()] = it->value().ToString();
            count++;
        }
        delete it;

        seg->state = SegmentState::HOT_BPLUS;
        std::cout << "[MORPH] ✅ Segment [" << segment_id << "] is now HOT_BPLUS! Pulled " << count << " keys into RAM.\n";
    }

    void monitor_loop() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2000)); 
            if (!running) break;

            for (auto& seg : segments) {
                if (seg->state == SegmentState::COLD_LSM) {
                    uint64_t reads = seg->read_count.exchange(0);
                    uint64_t writes = seg->write_count.exchange(0);
                    uint64_t total = reads + writes;

                    if (total > 0) {
                        double read_ratio = static_cast<double>(reads) / total;
                        double transformation_cost = KEYS_PER_SEGMENT * 1.0; 
                        double expected_gain = reads * 2.5; 

                        if (reads > 500 && read_ratio > 0.7 && expected_gain > transformation_cost) {
                            int target_id = seg->segment_id;
                            std::thread(&AHATree::Morph, this, target_id).detach();
                        }
                    }
                }
            }
        }
    }

public:
    AHATree(int total_expected_keys) {
        int num_segments = (total_expected_keys / KEYS_PER_SEGMENT) + 1;
        for (int i = 0; i < num_segments; i++) segments.push_back(std::make_unique<AdaptiveSegment>(i));
        
        std::filesystem::create_directories("aha_rocks_data");
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB::Open(options, "aha_rocks_data", &rocks_db);
        
        monitor_thread = std::thread(&AHATree::monitor_loop, this);
    }

    ~AHATree() {
        running = false;
        if (monitor_thread.joinable()) monitor_thread.join();
        delete rocks_db;
    }

    // --- SPRINT 4: THE CONSISTENCY GUARD ---
    void Put(const std::string& key, const std::string& value) {
        int seg_id = get_segment_id(key);
        auto& seg = segments[seg_id];
        
        seg->write_count.fetch_add(1, std::memory_order_relaxed);
        
        // 1. ALWAYS persist to the Cold LSM (RocksDB)
        rocks_db->Put(rocksdb::WriteOptions(), key, value);

        // 2. CONSISTENCY GUARD: If the segment is in RAM, update it there too!
        if (seg->state == SegmentState::HOT_BPLUS) {
            std::unique_lock<std::shared_mutex> lock(seg->rw_lock);
            if (seg->state == SegmentState::HOT_BPLUS) { // Double-check lock
                seg->hot_index[key] = value;
            }
        }
    }

    // --- SPRINT 4: DUAL-PATH ROUTING ---
    void Scan(const std::string& start_key, const std::string& end_key) {
        int seg_id = get_segment_id(start_key);
        auto& seg = segments[seg_id];
        
        seg->read_count.fetch_add(1, std::memory_order_relaxed);

        // 1. THE FAST PATH (RAM)
        if (seg->state == SegmentState::HOT_BPLUS) {
            std::shared_lock<std::shared_mutex> lock(seg->rw_lock);
            if (seg->state == SegmentState::HOT_BPLUS) { 
                // std::map allows us to quickly grab a sub-range (O(log N))
                auto it_start = seg->hot_index.lower_bound(start_key);
                auto it_end = seg->hot_index.upper_bound(end_key);
                for (auto it = it_start; it != it_end; ++it) {
                    volatile size_t s = it->second.size(); 
                }
                return; // Early exit! We didn't touch RocksDB.
            }
        }

        // 2. THE SLOW PATH (DISK)
        rocksdb::Iterator* it = rocks_db->NewIterator(rocksdb::ReadOptions());
        for (it->Seek(start_key); it->Valid() && it->key().compare(end_key) <= 0; it->Next()) {
            volatile auto val = it->value();
        }
        delete it;
    }

    void DebugPrintSegmentState(int segment_id) {
        auto& seg = segments[segment_id];
        std::shared_lock<std::shared_mutex> lock(seg->rw_lock);
        std::string state_str = (seg->state == SegmentState::HOT_BPLUS) ? "🔥 HOT_BPLUS" : "❄️ COLD_LSM";
        std::cout << "Segment [" << segment_id << "] -> State: " << state_str 
                  << " | Keys in RAM: " << seg->hot_index.size() << "\n";
    }
};
