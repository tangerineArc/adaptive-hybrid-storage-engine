#include "AdaptiveRouter.h"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <filesystem>
#include <lmdb.h>
#include <map>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/types.h>
#include <rocksdb/write_batch.h>
#include <shared_mutex>
#include <string>

// A reserved key that will never collide with normal user keys
// (assuming your user keys don't start with double underscores).
constexpr char SYSTEM_SEQ_KEY[] = "__SYSTEM_SEQ__";

// The Background Worker Handler
class LmdbIndexBuilder : public rocksdb::WriteBatch::Handler {
private:
  MDB_txn* txn;
  MDB_dbi dbi;
  std::map<std::string, std::optional<std::string>>& buffer;
  std::shared_mutex& buffer_mutex;

public:
  LmdbIndexBuilder(MDB_txn* transaction, MDB_dbi database,
                   std::map<std::string, std::optional<std::string>>& recent_writes,
                   std::shared_mutex& mutex)
    : txn(transaction), dbi(database), buffer(recent_writes), buffer_mutex(mutex) {}

  // Called when the log contains a PUT
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    MDB_val mdb_key, mdb_val;

    mdb_key.mv_size = key.size();
    mdb_key.mv_data = (void*)key.data();

    // Storing an empty value to save space. LMDB is just our index.
    mdb_val.mv_size = 0;
    mdb_val.mv_data = nullptr;

    mdb_put(txn, dbi, &mdb_key, &mdb_val, 0);

    // Erase from the Read-Your-Writes Buffer
    {
      std::unique_lock<std::shared_mutex> lock(buffer_mutex);
      buffer.erase(key.ToString());
    }
    return rocksdb::Status::OK();
  }

  // Called when the log contains a DELETE
  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    MDB_val mdb_key;
    mdb_key.mv_size = key.size();
    mdb_key.mv_data = (void*)key.data();

    mdb_del(txn, dbi, &mdb_key, nullptr);

    // Erase from the Read-Your-Writes Buffer
    {
      std::unique_lock<std::shared_mutex> lock(buffer_mutex);
      buffer.erase(key.ToString());
    }
    return rocksdb::Status::OK();
  }
};


AdaptiveRouter::AdaptiveRouter(const std::string& base_path) : rocks_db(nullptr), lmdb_env(nullptr) {
  std::string rocks_path = base_path + "/rocksdb_data";
  std::string lmdb_path = base_path + "/lmdb_data";

  // Ensure directories exist
  std::filesystem::create_directories(rocks_path);
  std::filesystem::create_directories(lmdb_path);

  // ==========================================
  //  Initialize RocksDB
  // ==========================================
  rocksdb::Options rocks_options;
  rocks_options.create_if_missing = true;
  // The WAL is enabled by default in RocksDB, which we need for Phase 3.

  rocksdb::Status status = rocksdb::DB::Open(rocks_options, rocks_path, &rocks_db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
  }

  // ==========================================
  //  Initialize LMDB
  // ==========================================
  if (mdb_env_create(&lmdb_env) != MDB_SUCCESS) {
    throw std::runtime_error("Failed to create LMDB environment");
  }

  // Set map size to 1GB (adjust this based on expected index size)
  // LMDB requires this to reserve virtual memory space.
  mdb_env_set_mapsize(lmdb_env, 1024UL * 1024UL * 1024UL);

  // Open the environment
  if (mdb_env_open(lmdb_env, lmdb_path.c_str(), 0, 0664) != MDB_SUCCESS) {
    throw std::runtime_error("Failed to open LMDB environment");
  }

  // To open an LMDB database, we must start a transaction
  MDB_txn* txn;
  if (mdb_txn_begin(lmdb_env, nullptr, 0, &txn) != MDB_SUCCESS) {
    throw std::runtime_error("Failed to begin LMDB transaction");
  }

  // Open the default database handle
  if (mdb_dbi_open(txn, nullptr, MDB_CREATE, &lmdb_dbi) != MDB_SUCCESS) {
    throw std::runtime_error("Failed to open LMDB database handle");
  }

  // Commit the transaction to save the database handle
  mdb_txn_commit(txn);

  // ==========================================
  //  Lifecycle Management (Start Thread)
  // ==========================================
  // Load where we left off (Implementation depends on you:
  // e.g., reading an integer from a local text file "seq.meta")
  last_processed_seq = LoadSequenceNumber();

  // Start the background worker
  keep_running.store(true);
  background_worker = std::thread(&AdaptiveRouter::TailTransactionLog, this);

  std::cout << "AdaptiveRouter initialized. Background indexer running." << std::endl;
}

AdaptiveRouter::~AdaptiveRouter() {
  // Gracefully shut down the background thread
  keep_running.store(false);
  if (background_worker.joinable()) {
    background_worker.join();
  }

  // Clean up LMDB
  if (lmdb_env) {
    mdb_dbi_close(lmdb_env, lmdb_dbi);
    mdb_env_close(lmdb_env);
  }

  // Clean up RocksDB
  if (rocks_db) {
    delete rocks_db;
  }
}

// The Write Path
bool AdaptiveRouter::Put(const std::string& key, const std::string& value) {
  if (!rocks_db) return false;

  // Add to the Read-Your-Writes buffer
  {
    std::unique_lock<std::shared_mutex> lock(buffer_mutex);
    recent_writes_buffer[key] = value;
  }

  rocksdb::WriteOptions write_options;

  // By default, RocksDB writes to the MemTable and an OS-buffered Write-Ahead Log (WAL).
  // If you need strict crash consistency (surviving a sudden power loss),
  // you can force an fsync by uncommenting the next line, at the cost of write throughput.
  // write_options.sync = true;

  rocksdb::Status status = rocks_db->Put(write_options, key, value);

  if (!status.ok()) {
    std::cerr << "RocksDB Put failed: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}

bool AdaptiveRouter::Delete(const std::string& key) {
  if (!rocks_db) return false;

  // Write the Tombstone to the buffer
  {
    std::unique_lock<std::shared_mutex> lock(buffer_mutex);
    recent_writes_buffer[key] = std::nullopt;
  }

  rocksdb::WriteOptions write_options;

  // Delete in an LSM-tree is actually a write operation (inserting a "tombstone").
  // It undergoes the exact same MemTable/WAL process as a Put.
  rocksdb::Status status = rocks_db->Delete(write_options, key);

  if (!status.ok()) {
    std::cerr << "RocksDB Delete failed: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}

bool AdaptiveRouter::Get(const std::string& key, std::string& value) {
  if (!rocks_db) return false;

  rocksdb::ReadOptions read_options;
  rocksdb::Status status = rocks_db->Get(read_options, key, &value);

  return status.ok();
}

// The Smart Read Path
std::vector<std::pair<std::string, std::string>> AdaptiveRouter::Scan(const std::string& start_key, const std::string& end_key) {
  std::vector<std::pair<std::string, std::string>> results;
  if (!lmdb_env || !rocks_db) return results;

  // ==========================================
  //  Fetch from LMDB (The Disk Index)
  // ==========================================

  // Open a Read-Only LMDB Transaction
  MDB_txn* txn;
  mdb_txn_begin(lmdb_env, nullptr, MDB_RDONLY, &txn);

  MDB_cursor* cursor;
  mdb_cursor_open(txn, lmdb_dbi, &cursor);

  MDB_val k, v;
  k.mv_size = start_key.size();
  k.mv_data = (void*)start_key.data();

  // Seek to the first key >= start_key
  int rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
  std::vector<std::string> keys_to_fetch;

  // Iterate sequentially through the B+Tree
  while (rc == MDB_SUCCESS) {
    std::string current_key((char*)k.mv_data, k.mv_size);

    // Stop if we have passed the end_key bound
    if (current_key > end_key) break;

    // CRITICAL: Filter out our internal metadata key!
    if (current_key != SYSTEM_SEQ_KEY) {
      keys_to_fetch.push_back(current_key);
    }

    // Move to the next leaf node
    rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
  }

  // Clean up LMDB resources
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  // ==========================================
  //  Fetch Values from RocksDB
  // ==========================================

  std::vector<std::string> disk_values;
  std::vector<rocksdb::Status> statuses;

  // The Multi-Get Step
  if (!keys_to_fetch.empty()) {
    std::vector<rocksdb::Slice> slices_to_fetch;
    slices_to_fetch.reserve(keys_to_fetch.size());
    for (const auto& key_str : keys_to_fetch) {
      slices_to_fetch.push_back(rocksdb::Slice(key_str));
    }

    rocksdb::ReadOptions read_options;
    // MultiGet parallelizes the disk I/O and cache lookups inside RocksDB
    statuses = rocks_db->MultiGet(read_options, slices_to_fetch, &disk_values);
  }

  // ==========================================
  //  The Two-Pointer Merge (Disk + Buffer)
  // ==========================================

  // Acquire a shared lock. Multiple Scans can read simultaneously.
  // It only blocks if a Put is actively writing to the buffer.
  std::shared_lock<std::shared_mutex> lock(buffer_mutex);

  // Get an iterator to the start of our range in the memory buffer
  auto buf_it = recent_writes_buffer.lower_bound(start_key);

  size_t disk_idx = 0;
  while (disk_idx < keys_to_fetch.size() || (buf_it != recent_writes_buffer.end() && buf_it->first <= end_key)) {
    // Check if the pointers are physically in bounds
    bool disk_exists = disk_idx < keys_to_fetch.size();
    bool buf_exists = (buf_it != recent_writes_buffer.end()) && (buf_it->first <= end_key);

    if (disk_exists && buf_exists) {
      if (keys_to_fetch[disk_idx] == buf_it->first) {
        // Collision: Buffer is fresher, so it wins.
        if (buf_it->second.has_value()) {
          results.push_back({buf_it->first, buf_it->second.value()});
        }
        disk_idx++;
        buf_it++;
      } else if (keys_to_fetch[disk_idx] < buf_it->first) {
        // Disk key is smaller, process it first to maintain sorted order
        if (statuses[disk_idx].ok()) {
          results.push_back({keys_to_fetch[disk_idx], disk_values[disk_idx]});
        }
        disk_idx++;
      } else {
        // Buffer key is smaller
        if (buf_it->second.has_value()) {
          results.push_back({buf_it->first, buf_it->second.value()});
        }
        buf_it++;
      }
    } else if (disk_exists) {
      // Only disk keys remain
      if (statuses[disk_idx].ok()) {
        results.push_back({keys_to_fetch[disk_idx], disk_values[disk_idx]});
      }
      disk_idx++;
    } else if (buf_exists) {
      // Only buffer keys remain
      if (buf_it->second.has_value()) {
        results.push_back({buf_it->first, buf_it->second.value()});
      }
      buf_it++;
    }
  }

  return results;
}

void AdaptiveRouter::TailTransactionLog() {
  while (keep_running.load()) {
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;

    // Ask RocksDB for everything that happened since our last recorded sequence
    rocksdb::Status status = rocks_db->GetUpdatesSince(last_processed_seq, &iter);

    if (status.ok() && iter->Valid()) {
      // Get the atomic batch of writes
      rocksdb::BatchResult batch = iter->GetBatch();

      // 1. Begin an LMDB Write Transaction
      MDB_txn* txn;
      mdb_txn_begin(lmdb_env, nullptr, 0, &txn);

      // 2. Feed the RocksDB batch into our custom LMDB Handler
      LmdbIndexBuilder handler(txn, lmdb_dbi, recent_writes_buffer, buffer_mutex);
      rocksdb::Status s = batch.writeBatchPtr->Iterate(&handler);

      if (s.ok()) {
        // 3. Calculate the next sequence number
        last_processed_seq = batch.sequence + batch.writeBatchPtr->Count();

        // 4. ATOMIC CHECKPOINT: Save the sequence in the SAME transaction
        SaveSequenceNumber(txn, last_processed_seq);

        // 5. Commit everything together
        mdb_txn_commit(txn);
      } else {
        // If anything went wrong, roll back both the keys and the sequence update
        mdb_txn_abort(txn);
        std::cerr << "Failed to parse WriteBatch: " << s.ToString() << std::endl;
      }

      // Move to the next batch in the log
      iter->Next();
    } else {
      // No new updates. Sleep for 50ms to yield CPU.
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
}

rocksdb::SequenceNumber AdaptiveRouter::LoadSequenceNumber() {
  MDB_txn* txn;
  // Open a Read-Only transaction
  mdb_txn_begin(lmdb_env, nullptr, MDB_RDONLY, &txn);

  MDB_val key, data;
  key.mv_size = std::strlen(SYSTEM_SEQ_KEY);
  key.mv_data = (void*)SYSTEM_SEQ_KEY;

  rocksdb::SequenceNumber seq = 0; // Default to 0 on first run
  // Try to fetch the sequence number
  if (mdb_get(txn, lmdb_dbi, &key, &data) == MDB_SUCCESS) {
    if (data.mv_size == sizeof(rocksdb::SequenceNumber)) {
      // Copy the 8 bytes from LMDB back into our uint64_t variable
      std::memcpy(&seq, data.mv_data, sizeof(rocksdb::SequenceNumber));
    }
  }

  // Abort is the standard way to close a Read-Only LMDB transaction
  mdb_txn_abort(txn);

  return seq;
}

void AdaptiveRouter::SaveSequenceNumber(MDB_txn* txn, rocksdb::SequenceNumber seq) {
  MDB_val key, data;

  key.mv_size = std::strlen(SYSTEM_SEQ_KEY);
  key.mv_data = (void*)SYSTEM_SEQ_KEY;

  data.mv_size = sizeof(rocksdb::SequenceNumber);
  data.mv_data = (void*)&seq;

  // Write it to LMDB as part of the currently open transaction
  mdb_put(txn, lmdb_dbi, &key, &data, 0);
}
