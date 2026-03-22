#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <lmdb.h>
#include <string>
#include <vector>
#include <stdexcept>

class AdaptiveRouter {
private:
  // RocksDB handles
  rocksdb::DB* rocks_db;

  // LMDB handles
  MDB_env* lmdb_env;
  MDB_dbi lmdb_dbi;

public:
  AdaptiveRouter(const std::string& db_path);
  ~AdaptiveRouter();

  // Prevent copying because we manage raw database pointers
  AdaptiveRouter(const AdaptiveRouter&) = delete;
  AdaptiveRouter& operator=(const AdaptiveRouter&) = delete;

  // The Unified CRUD API
  bool Put(const std::string& key, const std::string& value);
  bool Get(const std::string& key, std::string& value);
  bool Delete(const std::string& key);

  // Scan returns a vector of keys for now (can be expanded to key-value pairs)
  std::vector<std::string> Scan(const std::string& start_key, const std::string& end_key);
};
