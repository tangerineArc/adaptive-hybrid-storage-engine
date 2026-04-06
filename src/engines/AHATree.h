#pragma once

#include "Engine.h"
#include "../utils/Telemetry.h"

#include <atomic>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <shared_mutex>
#include <thread>
#include <vector>
#include <absl/container/btree_map.h>

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
  absl::btree_map<std::string, std::string> hot_index;

  AdaptiveSegment(int id) : segment_id(id), state(SegmentState::COLD_LSM) {}
};

class AhaTree : public Engine {
private:
  rocksdb::DB* rocks_db;
  std::vector<std::unique_ptr<AdaptiveSegment>> segments;
  const uint64_t KEYS_PER_SEGMENT = 1000;
  Telemetry& tracker;
  rocksdb::WriteOptions write_opts;

  std::atomic<bool> running{true};
  std::thread monitor_thread;

  // Decode the 8-byte Big-Endian key back into a uint64_t for routing
  int get_segment_id(const std::string& key) {
    if (key.size() != sizeof(uint64_t)) return 0;

    uint64_t swapped;
    std::memcpy(&swapped, key.data(), sizeof(uint64_t));
    uint64_t ts = __builtin_bswap64(swapped);

    int seg_id = ts / KEYS_PER_SEGMENT;
    if (seg_id >= (int)segments.size()) return segments.size() - 1;
    return seg_id;
  }

  std::string format_key_internal(uint64_t ts) {
    uint64_t swapped = __builtin_bswap64(ts);
    return std::string(reinterpret_cast<const char*>(&swapped), sizeof(swapped));
  }

  void Morph(int segment_id) {
    auto& seg = segments[segment_id];
    std::unique_lock<std::shared_mutex> lock(seg->rw_lock);

    if (seg->state == SegmentState::HOT_BPLUS) return;

    tracker.log_discrete_event("AHA", "MORPH_START");

    std::string start_key = format_key_internal(segment_id * KEYS_PER_SEGMENT);
    std::string end_key = format_key_internal((segment_id + 1) * KEYS_PER_SEGMENT - 1);

    rocksdb::Iterator* it = rocks_db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(start_key); it->Valid() && it->key().compare(end_key) <= 0; it->Next()) {
      seg->hot_index[it->key().ToString()] = it->value().ToString();
    }
    delete it;

    seg->state = SegmentState::HOT_BPLUS;
    tracker.log_discrete_event("AHA", "MORPH_END");
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
            double expected_gain = reads * 0.04;
            double transformation_cost = KEYS_PER_SEGMENT * 0.02;

            if (expected_gain > transformation_cost && read_ratio > 0.7) {
              int target_id = seg->segment_id;
              std::thread(&AhaTree::Morph, this, target_id).detach();
            }
          }
        }
      }
    }
  }

public:
  AhaTree(uint64_t total_expected_keys, Telemetry& t) : tracker(t) {
    int num_segments = (total_expected_keys / KEYS_PER_SEGMENT) + 1;
    for (int i = 0; i < num_segments; i++) {
      segments.push_back(std::make_unique<AdaptiveSegment>(i));
    }

    std::filesystem::remove_all("data_aha_rocks");
    std::filesystem::create_directories("data_aha_rocks");
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB::Open(options, "data_aha_rocks", &rocks_db);

    write_opts.sync = false;
    write_opts.disableWAL = false;

    monitor_thread = std::thread(&AhaTree::monitor_loop, this);
  }

  ~AhaTree() {
    running = false;
    if (monitor_thread.joinable()) monitor_thread.join();
    delete rocks_db;
  }

  std::string GetName() override { return "AHA"; }

  void Put(const std::string& key, const std::string& value) override {
    int seg_id = get_segment_id(key);
    auto& seg = segments[seg_id];

    seg->write_count.fetch_add(1, std::memory_order_relaxed);
    rocks_db->Put(write_opts, key, value);

    if (seg->state == SegmentState::HOT_BPLUS) {
      std::unique_lock<std::shared_mutex> lock(seg->rw_lock);
      if (seg->state == SegmentState::HOT_BPLUS) {
        seg->hot_index[key] = value;
      }
    }
  }

  void Scan(const std::string& start_key, const std::string& end_key) override {
    int seg_id = get_segment_id(start_key);
    auto& seg = segments[seg_id];

    seg->read_count.fetch_add(1, std::memory_order_relaxed);

    if (seg->state == SegmentState::HOT_BPLUS) {
      std::shared_lock<std::shared_mutex> lock(seg->rw_lock);
      if (seg->state == SegmentState::HOT_BPLUS) {
        auto it_start = seg->hot_index.lower_bound(start_key);
        auto it_end = seg->hot_index.upper_bound(end_key);
        for (auto it = it_start; it != it_end; ++it) {
          volatile size_t s = it->second.size();
        }
        return;
      }
    }

    rocksdb::Iterator* it = rocks_db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(start_key); it->Valid() && it->key().compare(end_key) <= 0; it->Next()) {
      volatile auto val = it->value();
    }
    delete it;
  }
};
