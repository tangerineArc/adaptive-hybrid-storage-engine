#pragma once

#include "WorkloadMonitor.h"
#include <lmdb.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <atomic>
#include <filesystem>
#include <iostream>
#include <shared_mutex>
#include <string>
#include <thread>

class StorageManager {
private:
  // 1. The Engines
  rocksdb::DB *rocks_db;
  MDB_env *lmdb_env;
  MDB_dbi lmdb_dbi;

  // 2. The Brain
  WorkloadMonitor monitor;

  // 3. Thread Safety & State
  std::atomic<bool> is_lmdb_ready{false};
  std::atomic<bool> is_migrating{false};

  // This lock prevents the system from deleting the LMDB index
  // while active queries (reads or dual-writes) are using it.
  std::shared_mutex index_rw_lock;

  // --- Phase 2: The Background Builder (Scanning D_old) ---
  void build_lmdb_index() {
    if (is_migrating)
      return;

    std::cout << "\n[MIGRATION] ⏸️  Phase 1: Snapping RocksDB state...\n";

    // 1. Take the Snapshot (The Frozen Set)
    const rocksdb::Snapshot *snapshot = rocks_db->GetSnapshot();

    // 2. Flip the switch to initiate Dual-Writing (The Live Set)
    is_migrating = true;
    std::cout << "[MIGRATION] 🔀 Phase 2: Dual-write enabled. Building index "
                 "in background...\n";

    // 3. Set up RocksDB Iterator to only look at the Snapshot
    rocksdb::ReadOptions read_opts;
    read_opts.snapshot = snapshot;
    rocksdb::Iterator *it = rocks_db->NewIterator(read_opts);

    // Set up LMDB Transaction
    MDB_txn *txn;
    mdb_txn_begin(lmdb_env, NULL, 0, &txn);

    int count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      MDB_val k, v;
      k.mv_size = it->key().size();
      k.mv_data = (void *)it->key().data();
      v.mv_size = it->value().size();
      v.mv_data = (void *)it->value().data();

      // ⚠️ THE COLLISION LOGIC: MDB_NOOVERWRITE
      // If the live thread already dual-wrote this key, LMDB will return
      // MDB_KEYEXIST and safely ignore this older snapshot value.
      // First-Write-Wins!
      mdb_put(txn, lmdb_dbi, &k, &v, MDB_NOOVERWRITE);

      count++;
      // Commit in batches of 10,000 to keep memory footprint low
      if (count % 10000 == 0) {
        mdb_txn_commit(txn);
        mdb_txn_begin(lmdb_env, NULL, 0, &txn);
      }
    }
    mdb_txn_commit(txn);
    delete it;

    // 4. Release the Snapshot
    rocks_db->ReleaseSnapshot(snapshot);

    // 5. Phase 3: Convergence
    is_lmdb_ready = true;
    is_migrating = false;
    std::cout << "[MIGRATION] ✅ Phase 3: LMDB Index Built (" << count
              << " keys). Routing scans to B+Tree.\n";
  }

  void drop_lmdb_index() {
    if (!is_lmdb_ready)
      return;

    // Exclusive Lock: Wait for all current Reads and Puts to finish,
    // then block anyone else from touching LMDB while we destroy it.
    std::unique_lock<std::shared_mutex> lock(index_rw_lock);

    is_lmdb_ready = false;
    is_migrating = false;

    std::cout << "\n[SYSTEM] 🗑️ Dropping LMDB Index to save RAM.\n";

    // Empty the LMDB database (0 deletes the data, 1 would delete the DB
    // instance entirely)
    MDB_txn *txn;
    mdb_txn_begin(lmdb_env, NULL, 0, &txn);
    mdb_drop(txn, lmdb_dbi, 0);
    mdb_txn_commit(txn);
  }

public:
  StorageManager() : monitor(2000) { // 2-second window for faster testing
    // Init RocksDB
    std::filesystem::create_directories("data_rocks");
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB::Open(options, "data_rocks", &rocks_db);

    // Init LMDB
    std::filesystem::create_directories("data_lmdb");
    mdb_env_create(&lmdb_env);
    mdb_env_set_mapsize(lmdb_env, 1ULL * 1024 * 1024 * 1024); // 1GB
    mdb_env_open(lmdb_env, "./data_lmdb", MDB_NOSYNC, 0664);

    MDB_txn *txn;
    mdb_txn_begin(lmdb_env, NULL, 0, &txn);
    mdb_dbi_open(txn, NULL, MDB_CREATE, &lmdb_dbi);
    mdb_txn_commit(txn);
  }

  ~StorageManager() {
    delete rocks_db;
    mdb_dbi_close(lmdb_env, lmdb_dbi);
    mdb_env_close(lmdb_env);
  }

  // ==========================================
  // THE ROUTER LOGIC
  // ==========================================

  void Put(const std::string &key, const std::string &value) {
    monitor.record_write();

    // 1. Always write to Primary (RocksDB)
    rocks_db->Put(rocksdb::WriteOptions(), key, value);

    // 2. The Dual-Write Path (Intercepting D_new)
    // If we are fully ready OR actively migrating, we must mirror the write to
    // LMDB.
    if (is_lmdb_ready || is_migrating) {

      // Shared lock: Multiple threads can safely dual-write at the same time
      std::shared_lock<std::shared_mutex> lock(index_rw_lock);

      // Double check state inside the lock to be safe
      if (is_lmdb_ready || is_migrating) {
        MDB_txn *txn;
        mdb_txn_begin(lmdb_env, NULL, 0, &txn);

        MDB_val k, v;
        k.mv_size = key.size();
        k.mv_data = (void *)key.data();
        v.mv_size = value.size();
        v.mv_data = (void *)value.data();

        // Live writes ALWAYS overwrite (0 flag), because they are the newest
        // data
        mdb_put(txn, lmdb_dbi, &k, &v, 0);
        mdb_txn_commit(txn);
      }
    }
  }

  void Scan(const std::string &start_key, const std::string &end_key) {
    monitor.record_read();

    // 1. Consult the Brain
    WorkloadState state = monitor.get_state();

    // 2. State Machine Logic
    if (state == WorkloadState::READ_HEAVY && !is_lmdb_ready && !is_migrating) {
      std::thread(&StorageManager::build_lmdb_index, this).detach();
    } else if (state == WorkloadState::WRITE_HEAVY && is_lmdb_ready) {
      drop_lmdb_index();
    }

    // 3. The Query Routing
    if (is_lmdb_ready) {
      // --- ROUTE TO B+TREE (Fast Path) ---
      std::shared_lock<std::shared_mutex> lock(index_rw_lock);
      if (is_lmdb_ready) { // Check again inside lock
        MDB_txn *txn;
        mdb_txn_begin(lmdb_env, NULL, MDB_RDONLY, &txn);
        MDB_cursor *cursor;
        mdb_cursor_open(txn, lmdb_dbi, &cursor);

        MDB_val k, v;
        k.mv_size = start_key.size();
        k.mv_data = (void *)start_key.data();

        if (mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE) == MDB_SUCCESS) {
          do {
            if (memcmp(k.mv_data, end_key.data(), k.mv_size) > 0)
              break;
            volatile size_t s = v.mv_size; // Touch data
          } while (mdb_cursor_get(cursor, &k, &v, MDB_NEXT) == MDB_SUCCESS);
        }
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        return; // Scan complete!
      }
    }

    // --- ROUTE TO LSM-TREE (Fallback / Default Path) ---
    rocksdb::Iterator *it = rocks_db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(start_key); it->Valid() && it->key().compare(end_key) <= 0;
         it->Next()) {
      volatile auto val = it->value(); // Touch data
    }
    delete it;
  }

  void print_internal_memory_stats() {
    std::string memtable_usage, block_cache, readers_mem;
    rocks_db->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_usage);
    rocks_db->GetProperty("rocksdb.block-cache-usage", &block_cache);
    rocks_db->GetProperty("rocksdb.estimate-table-readers-mem", &readers_mem);

    uint64_t r_mem = std::stoull(memtable_usage) + std::stoull(block_cache) +
                     std::stoull(readers_mem);

    std::cout << "  -> RocksDB Internal Memory: " << (r_mem / 1024.0 / 1024.0)
              << " MB\n";

    if (is_lmdb_ready || is_migrating) {
      MDB_envinfo env_info;
      MDB_stat env_stat;
      mdb_env_info(lmdb_env, &env_info);
      mdb_env_stat(lmdb_env, &env_stat);

      size_t actual_bytes_used = env_stat.ms_psize * env_info.me_last_pgno;
      std::cout << "  -> LMDB Actual Mapped Data: "
                << (actual_bytes_used / 1024.0 / 1024.0) << " MB\n";
    } else {
      std::cout << "  -> LMDB is offline (0 MB).\n";
    }
  }
};
