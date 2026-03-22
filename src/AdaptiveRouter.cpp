#include "AdaptiveRouter.h"
#include <iostream>
#include <filesystem>

AdaptiveRouter::AdaptiveRouter(const std::string& base_path) : rocks_db(nullptr), lmdb_env(nullptr) {
  std::string rocks_path = base_path + "/rocksdb_data";
  std::string lmdb_path = base_path + "/lmdb_data";

  // Ensure directories exist
  std::filesystem::create_directories(rocks_path);
  std::filesystem::create_directories(lmdb_path);

  // ==========================================
  // 1. Initialize RocksDB
  // ==========================================
  rocksdb::Options rocks_options;
  rocks_options.create_if_missing = true;
  // The WAL is enabled by default in RocksDB, which we need for Phase 3.

  rocksdb::Status status = rocksdb::DB::Open(rocks_options, rocks_path, &rocks_db);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
  }

  // ==========================================
  // 2. Initialize LMDB
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

// Stub methods for the API
bool AdaptiveRouter::Put(const std::string& key, const std::string& value) { return false; }
bool AdaptiveRouter::Get(const std::string& key, std::string& value) { return false; }
bool AdaptiveRouter::Delete(const std::string& key) { return false; }
std::vector<std::string> AdaptiveRouter::Scan(const std::string& start_key, const std::string& end_key) {
  return {};
}
