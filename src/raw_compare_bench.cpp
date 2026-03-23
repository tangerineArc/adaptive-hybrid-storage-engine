#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <lmdb.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <random>
#include <filesystem>

const int NUM_RECORDS = 1'000'000;
const int LMDB_BATCH_SIZE = 10'000; // Commit every 10k records
const int NUM_SCANS = 10'000;
const int SCAN_LENGTH = 100;

// Format keys to strictly 16 bytes for fair comparison
std::string MakeKey(int i) {
  std::ostringstream oss;
  oss << "user_" << std::setw(11) << std::setfill('0') << i;
  return oss.str();
}

int main() {
  std::cout << "--- Pre-generating and Shuffling Data ---\n";
  std::vector<std::string> keys;
  keys.reserve(NUM_RECORDS);
  for (int i = 0; i < NUM_RECORDS; ++i) {
      keys.push_back(MakeKey(i));
  }

  // Shuffle to simulate real-world random inserts (UUIDs, hashes, etc.)
  // This forces LMDB into expensive B-Tree page splits.
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  std::string payload(100, 'x'); // 100 bytes of dummy data

  // Clean up old databases
  std::filesystem::remove_all("./bench_rocks");
  std::filesystem::remove_all("./bench_lmdb");
  std::filesystem::create_directories("./bench_rocks");
  std::filesystem::create_directories("./bench_lmdb");

  // ==========================================
  // 1. ROCKSDB: Random Insertions
  // ==========================================
  rocksdb::DB* rocks_db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB::Open(options, "./bench_rocks", &rocks_db);

  std::cout << "\nStarting RocksDB Inserts (" << NUM_RECORDS << " random keys)...\n";
  auto start_rocks_write = std::chrono::high_resolution_clock::now();

  rocksdb::WriteOptions write_opts;
  for (int i = 0; i < NUM_RECORDS; ++i) {
    rocks_db->Put(write_opts, keys[i], payload);
  }

  auto end_rocks_write = std::chrono::high_resolution_clock::now();
  auto rocks_write_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_rocks_write - start_rocks_write).count();
  std::cout << "RocksDB Insert Time: " << rocks_write_time << " ms\n";

  // ==========================================
  // 2. LMDB: Random Insertions
  // ==========================================
  MDB_env* lmdb_env;
  MDB_dbi lmdb_dbi;
  mdb_env_create(&lmdb_env);
  mdb_env_set_mapsize(lmdb_env, 2048UL * 1024UL * 1024UL); // 2GB to handle fragmentation
  mdb_env_open(lmdb_env, "./bench_lmdb", 0, 0664);

  MDB_txn* txn;
  mdb_txn_begin(lmdb_env, nullptr, 0, &txn);
  mdb_dbi_open(txn, nullptr, MDB_CREATE, &lmdb_dbi);

  std::cout << "\nStarting LMDB Inserts (" << NUM_RECORDS << " random keys, batched " << LMDB_BATCH_SIZE << ")...\n";
  auto start_lmdb_write = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_RECORDS; ++i) {
    MDB_val k{keys[i].size(), (void*)keys[i].data()};
    MDB_val v{payload.size(), (void*)payload.data()};

    mdb_put(txn, lmdb_dbi, &k, &v, 0);

    // Batch commits to simulate realistic transactional workload
    if ((i + 1) % LMDB_BATCH_SIZE == 0) {
      mdb_txn_commit(txn);
      mdb_txn_begin(lmdb_env, nullptr, 0, &txn);
    }
  }
  mdb_txn_commit(txn); // Commit any remainders

  auto end_lmdb_write = std::chrono::high_resolution_clock::now();
  auto lmdb_write_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_lmdb_write - start_lmdb_write).count();
  std::cout << "LMDB Insert Time:    " << lmdb_write_time << " ms\n";

  // ==========================================
  // 3. ROCKSDB: Range Scans
  // ==========================================
  std::cout << "\nStarting RocksDB Scans (" << NUM_SCANS << " scans of length " << SCAN_LENGTH << ")...\n";
  auto start_rocks_scan = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_SCANS; ++i) {
    // Pick a random starting point using our sequential formatter
    std::string start_key = MakeKey(rand() % (NUM_RECORDS - SCAN_LENGTH));

    rocksdb::Iterator* it = rocks_db->NewIterator(rocksdb::ReadOptions());
    int count = 0;
    for (it->Seek(start_key); it->Valid() && count < SCAN_LENGTH; it->Next()) {
        volatile size_t s = it->value().size(); // Prevent compiler optimization out
        count++;
    }
    delete it;
  }

  auto end_rocks_scan = std::chrono::high_resolution_clock::now();
  auto rocks_scan_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_rocks_scan - start_rocks_scan).count();
  std::cout << "RocksDB Scan Time: " << rocks_scan_time << " ms\n";

  // ==========================================
  // 4. LMDB: Range Scans
  // ==========================================
  std::cout << "\nStarting LMDB Scans (" << NUM_SCANS << " scans of length " << SCAN_LENGTH << ")...\n";
  auto start_lmdb_scan = std::chrono::high_resolution_clock::now();

  mdb_txn_begin(lmdb_env, nullptr, MDB_RDONLY, &txn);
  for (int i = 0; i < NUM_SCANS; ++i) {
    std::string start_key = MakeKey(rand() % (NUM_RECORDS - SCAN_LENGTH));

    MDB_cursor* cursor;
    mdb_cursor_open(txn, lmdb_dbi, &cursor);

    MDB_val k, v;
    k.mv_size = start_key.size();
    k.mv_data = (void*)start_key.data();

    int rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE);
    int count = 0;
    while (rc == MDB_SUCCESS && count < SCAN_LENGTH) {
      volatile size_t s = v.mv_size;
      count++;
      rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT);
    }
    mdb_cursor_close(cursor);
  }
  mdb_txn_abort(txn);

  auto end_lmdb_scan = std::chrono::high_resolution_clock::now();
  auto lmdb_scan_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_lmdb_scan - start_lmdb_scan).count();
  std::cout << "LMDB Scan Time:    " << lmdb_scan_time << " ms\n";

  // Cleanup
  delete rocks_db;
  mdb_env_close(lmdb_env);

  return 0;
}
