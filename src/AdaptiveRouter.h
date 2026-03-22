#pragma once

#include <atomic>
#include <lmdb.h>
#include <map>
#include <optional>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/types.h>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

class AdaptiveRouter {
private:
  // RocksDB handles
  rocksdb::DB* rocks_db;

  MDB_env* lmdb_env;
  MDB_dbi lmdb_dbi;

  std::thread background_worker;
  std::atomic<bool> keep_running;
  rocksdb::SequenceNumber last_processed_seq;

  // std::nullopt will represent our Tombstone
  std::map<std::string, std::optional<std::string>> recent_writes_buffer;
  // Allows multiple concurrent readers, single writer
  std::shared_mutex buffer_mutex;

  // The method that the thread will execute
  void TailTransactionLog();

  rocksdb::SequenceNumber LoadSequenceNumber();
  void SaveSequenceNumber(MDB_txn* txn, rocksdb::SequenceNumber seq);

public:
  AdaptiveRouter(const std::string& db_path);
  ~AdaptiveRouter();

  AdaptiveRouter(const AdaptiveRouter&) = delete;
  AdaptiveRouter& operator=(const AdaptiveRouter&) = delete;

  bool Put(const std::string& key, const std::string& value);
  bool Get(const std::string& key, std::string& value);
  bool Delete(const std::string& key);
  std::vector<std::pair<std::string, std::string>> Scan(const std::string& start_key, const std::string& end_key);
};
