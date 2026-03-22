#include "AdaptiveRouter.h"
#include <iostream>
#include <filesystem>
#include <rocksdb/options.h>

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

  std::cout << "AdaptiveRouter initialized successfully." << std::endl;
}

AdaptiveRouter::~AdaptiveRouter() {
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
