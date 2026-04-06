#pragma once

#include "Engine.h"

#include <filesystem>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

class RocksBaseline : public Engine {
private:
  rocksdb::DB* db;
  rocksdb::WriteOptions write_opts;

public:
  RocksBaseline() {
    std::filesystem::remove_all("data_baseline_rocks");
    rocksdb::Options opt;
    opt.create_if_missing = true;
    opt.write_buffer_size = 64 * 1024 * 1024;

    // 512MB Block Cache
    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_cache = rocksdb::NewLRUCache(512 * 1024 * 1024);
    opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

    rocksdb::DB::Open(opt, "data_baseline_rocks", &db);

    write_opts.sync = false;
    write_opts.disableWAL = false;
  }

  ~RocksBaseline() {
    delete db;
  }

  std::string GetName() override { return "ROCKSDB"; }

  std::string GetActiveBackend() override { return "BASE_ROCKSDB"; }

  void Put(const std::string& key, const std::string& value) override {
    db->Put(write_opts, key, value);
  }

  void Scan(const std::string& start_key, const std::string& end_key) override {
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek(start_key); it->Valid() && it->key().compare(end_key) <= 0; it->Next()) {
      volatile auto val = it->value();
    }
    delete it;
  }
};
