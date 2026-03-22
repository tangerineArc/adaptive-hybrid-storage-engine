#include "AdaptiveRouter.h"
#include <cstdint>
#include <cstring>
#include <iostream>
#include <filesystem>
#include <lmdb.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/types.h>
#include <rocksdb/write_batch.h>

// A reserved key that will never collide with normal user keys
// (assuming your user keys don't start with double underscores)
constexpr char SYSTEM_SEQ_KEY[] = "__SYSTEM_SEQ__";

class LmdbIndexBuilder : public rocksdb::WriteBatch::Handler {
private:
  MDB_txn* txn;
  MDB_dbi dbi;

public:
  LmdbIndexBuilder(MDB_txn* transaction, MDB_dbi database)
    : txn(transaction), dbi(database) {}

  // Called when the log contains a PUT
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    MDB_val mdb_key, mdb_val;

    mdb_key.mv_size = key.size();
    mdb_key.mv_data = (void*)key.data();

    // Storing an empty value to save space. LMDB is just our index.
    mdb_val.mv_size = 0;
    mdb_val.mv_data = nullptr;

    mdb_put(txn, dbi, &mdb_key, &mdb_val, 0);
    return rocksdb::Status::OK();
  }

  // Called when the log contains a DELETE
  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
    MDB_val mdb_key;
    mdb_key.mv_size = key.size();
    mdb_key.mv_data = (void*)key.data();

    mdb_del(txn, dbi, &mdb_key, nullptr);
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

bool AdaptiveRouter::Put(const std::string& key, const std::string& value) {
  if (!rocks_db) return false;

  rocksdb::WriteOptions write_options;

  // By default, RocksDB writes to the MemTable and an OS-buffered Write-Ahead Log (WAL).
  // If you need strict crash consistency (surviving a sudden power loss),
  // you can force an fsync by uncommenting the next line, at the cost of write throughput.
  // write_options.sync = true;

  // RocksDB natively accepts std::string via implicit conversion to rocksdb::Slice
  rocksdb::Status status = rocks_db->Put(write_options, key, value);

  if (!status.ok()) {
    std::cerr << "RocksDB Put failed: " << status.ToString() << std::endl;
    return false;
  }
  return true;
}

bool AdaptiveRouter::Delete(const std::string& key) {
  if (!rocks_db) return false;

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

// Stub methods for the API
bool AdaptiveRouter::Get(const std::string& key, std::string& value) { return false; }
std::vector<std::string> AdaptiveRouter::Scan(const std::string& start_key, const std::string& end_key) {
  return {};
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
      LmdbIndexBuilder handler(txn, lmdb_dbi);
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
