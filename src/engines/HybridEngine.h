#pragma once

#include "Engine.h"
#include "../utils/Telemetry.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <filesystem>
#include <iostream>
#include <lmdb.h>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <shared_mutex>
#include <thread>

enum class WorkloadState {
  STABLE,
  READ_HEAVY,
  WRITE_HEAVY
};

class HybridEngine : public Engine {
private:
  rocksdb::DB* rocks_db;
  rocksdb::WriteOptions write_opts;
  MDB_env* lmdb_env;
  MDB_dbi lmdb_dbi;
  Telemetry& tracker;

  std::atomic<bool> running{true};
  std::atomic<bool> is_lmdb_ready{false};
  std::atomic<bool> is_migrating{false};

  std::shared_mutex index_rw_lock;
  std::mutex lmdb_write_lock; // Explicit write lock to prevent LMDB thread contention

  WorkloadState current_state{WorkloadState::STABLE};
  std::chrono::steady_clock::time_point last_state_change;

  // The Brain (Stochastic Model)
  std::thread brain_thread;
  std::deque<double> read_ratios;
  std::mutex brain_lock;
  std::atomic<uint64_t> interval_reads{0};
  std::atomic<uint64_t> interval_writes{0};

  void trigger_migration() {
    if (is_migrating) return;

    if (tracker.current_ram_percent() > 90.0) {
      tracker.log_discrete_event("HYBRID", "MIGRATION_ABORT_RAM_90");
      std::cout << "[HYBRID] ⚠️ Migration aborted! RAM > 90%.\n";
      return;
    }

    is_migrating = true;
    tracker.log_discrete_event("HYBRID", "MIGRATION_START");
    auto start_mig = std::chrono::high_resolution_clock::now();
    std::cout << "\n[HYBRID] 🔀 Phase 1: Dual-write enabled. Building LMDB index...\n";

    const rocksdb::Snapshot* snapshot = rocks_db->GetSnapshot();
    rocksdb::ReadOptions read_opts;
    read_opts.snapshot = snapshot;
    rocksdb::Iterator* it = rocks_db->NewIterator(read_opts);

    MDB_txn* txn;
    mdb_txn_begin(lmdb_env, NULL, 0, &txn);

    int count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      MDB_val k, v;
      k.mv_size = it->key().size();
      k.mv_data = (void*)it->key().data();
      v.mv_size = it->value().size();
      v.mv_data = (void*)it->value().data();

      mdb_put(txn, lmdb_dbi, &k, &v, MDB_NOOVERWRITE);
      count++;
      if (count % 10000 == 0) {
        mdb_txn_commit(txn);
        mdb_txn_begin(lmdb_env, NULL, 0, &txn);
      }
    }
    mdb_txn_commit(txn);
    delete it;
    rocks_db->ReleaseSnapshot(snapshot);

    is_lmdb_ready = true;
    is_migrating = false;

    auto end_mig = std::chrono::high_resolution_clock::now();
    double mig_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_mig - start_mig).count();

    std::cout << "[HYBRID] ✅ Phase 3: LMDB Built (" << count << " keys) in " << mig_time << " ms.\n";
    tracker.log_discrete_event("HYBRID", "MIGRATION_END");
  }

  void drop_index() {
    if (!is_lmdb_ready) return;

    tracker.log_discrete_event("HYBRID", "REVERT");
    std::cout << "\n[HYBRID] 🗑️ Dropping LMDB Index due to Write-Heavy workload.\n";

    std::unique_lock<std::shared_mutex> lock(index_rw_lock);
    is_lmdb_ready = false;

    std::lock_guard<std::mutex> w_lock(lmdb_write_lock);
    MDB_txn* txn;
    mdb_txn_begin(lmdb_env, NULL, 0, &txn);
    mdb_drop(txn, lmdb_dbi, 0);
    mdb_txn_commit(txn);
  }

  void brain_loop() {
    while(running) {
      std::this_thread::sleep_for(std::chrono::seconds(2)); // Check frequently, sliding window spans 10s
      if (!running) break;

      uint64_t r = interval_reads.exchange(0);
      uint64_t w = interval_writes.exchange(0);
      double ratio = (r + w == 0) ? 0 : static_cast<double>(r) / (r + w);

      std::lock_guard<std::mutex> lock(brain_lock);
      read_ratios.push_back(ratio);
      if (read_ratios.size() > 5) read_ratios.pop_front();

      int high_count = 0;
      for (double x : read_ratios) {
        if (x > 0.8) high_count++; // Thigh
      }

      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::seconds>(now - last_state_change).count() < 10) continue;

      if (high_count >= 3 && current_state != WorkloadState::READ_HEAVY) {
        current_state = WorkloadState::READ_HEAVY;
        last_state_change = now;
        std::thread(&HybridEngine::trigger_migration, this).detach();
      } else if (high_count == 0 && ratio < 0.4 && current_state == WorkloadState::READ_HEAVY) {
        current_state = WorkloadState::WRITE_HEAVY;
        last_state_change = now;
        drop_index();
      }
    }
  }

public:
  HybridEngine(Telemetry& t) : tracker(t) {
    last_state_change = std::chrono::steady_clock::now();

    std::filesystem::remove_all("data_hybrid_rocks");
    std::filesystem::remove_all("data_hybrid_lmdb");
    std::filesystem::create_directories("data_hybrid_rocks");
    std::filesystem::create_directories("data_hybrid_lmdb");

    rocksdb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 64 * 1024 * 1024;
    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_cache = rocksdb::NewLRUCache(512 * 1024 * 1024);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));
    rocksdb::DB::Open(options, "data_hybrid_rocks", &rocks_db);

    write_opts.sync = false;
    write_opts.disableWAL = false;

    mdb_env_create(&lmdb_env);
    mdb_env_set_mapsize(lmdb_env, 1ULL * 1024 * 1024 * 1024);
    mdb_env_open(lmdb_env, "./data_hybrid_lmdb", MDB_NOSYNC, 0664);

    MDB_txn* txn;
    mdb_txn_begin(lmdb_env, NULL, 0, &txn);
    mdb_dbi_open(txn, NULL, MDB_CREATE, &lmdb_dbi);
    mdb_txn_commit(txn);

    brain_thread = std::thread(&HybridEngine::brain_loop, this);
  }

  ~HybridEngine() {
    running = false;
    if (brain_thread.joinable()) brain_thread.join();
    delete rocks_db;
    mdb_dbi_close(lmdb_env, lmdb_dbi);
    mdb_env_close(lmdb_env);
  }

  std::string GetName() override { return "HYBRID"; }

  std::string GetActiveBackend() override { return is_lmdb_ready ? "HYBRID_LMDB" : "HYBRID_ROCKSDB"; }

  void Put(const std::string& key, const std::string& value) override {
    interval_writes.fetch_add(1, std::memory_order_relaxed);
    rocks_db->Put(write_opts, key, value);

    if (is_lmdb_ready || is_migrating) {
      std::shared_lock<std::shared_mutex> lock(index_rw_lock);
      if (is_lmdb_ready || is_migrating) {
        std::lock_guard<std::mutex> w_lock(lmdb_write_lock); // Hard fix for LMDB concurrent transaction limits
        MDB_txn* txn;
        mdb_txn_begin(lmdb_env, NULL, 0, &txn);
        MDB_val k, v;
        k.mv_size = key.size();
        k.mv_data = (void*)key.data();
        v.mv_size = value.size();
        v.mv_data = (void*)value.data();
        mdb_put(txn, lmdb_dbi, &k, &v, 0);
        mdb_txn_commit(txn);
      }
    }
  }

  void Scan(const std::string& start_key, const std::string& end_key) override {
    interval_reads.fetch_add(1, std::memory_order_relaxed);

    if (is_lmdb_ready) {
      std::shared_lock<std::shared_mutex> lock(index_rw_lock);
      if (is_lmdb_ready) {
        MDB_txn* txn;
        mdb_txn_begin(lmdb_env, NULL, MDB_RDONLY, &txn);
        MDB_cursor* cursor;
        mdb_cursor_open(txn, lmdb_dbi, &cursor);

        MDB_val k, v;
        k.mv_size = start_key.size();
        k.mv_data = (void*)start_key.data();

        if (mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE) == MDB_SUCCESS) {
          do {
            if (memcmp(k.mv_data, end_key.data(), k.mv_size) > 0) break;
            volatile size_t s = v.mv_size;
          } while (mdb_cursor_get(cursor, &k, &v, MDB_NEXT) == MDB_SUCCESS);
        }
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
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
