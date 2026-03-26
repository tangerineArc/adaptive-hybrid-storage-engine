#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <lmdb.h>

#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <filesystem>
#include <iomanip>
#include <cstring>

using namespace std;
using namespace std::chrono;

const int N = 10000000;
const int BATCH_SIZE = 100; // Simulates real-world streaming ingestion
const int RANGE_SIZE = 2000;
const int QUERIES = 1000;

// ==============================
// True 8-byte Big-Endian Encoding
// ==============================
string encode_binary(uint64_t ts) {
    // __builtin_bswap64 is a fast, native compiler intrinsic for GCC/Clang
    uint64_t be_ts = __builtin_bswap64(ts); 
    return string(reinterpret_cast<const char*>(&be_ts), sizeof(be_ts));
}

// ==============================
// Random value generator
// ==============================
string random_string(int len = 16) {
    static mt19937 rng(42);
    uniform_int_distribution<int> dist(0, 25);
    string s;
    for (int i = 0; i < len; i++)
        s += char('a' + dist(rng));
    return s;
}

// ==============================
// Dataset
// ==============================
vector<pair<string,string>> dataset;

void generate_data() {
    dataset.reserve(N);
    for (int i = 0; i < N; i++) {
        dataset.push_back({encode_binary(i), random_string()});
    }
}

// ==============================
// RocksDB Range Query & Metrics
// ==============================
void rocks_range() {
    filesystem::remove_all("rocks_ts");

    rocksdb::DB* db;
    rocksdb::Options opt;
    opt.create_if_missing = true;
    
    // Recommended options for bulk inserts & reads
    opt.write_buffer_size = 64 * 1024 * 1024; // 64MB MemTable

    rocksdb::DB::Open(opt, "rocks_ts", &db);

    // 🔥 Measure Insert Time (Chunked Streaming)
    auto insert_start = high_resolution_clock::now();

    rocksdb::WriteBatch batch;
    for (int i = 0; i < N; i++) {
        batch.Put(dataset[i].first, dataset[i].second);
        
        // Flush the batch every BATCH_SIZE records
        if ((i + 1) % BATCH_SIZE == 0) {
            db->Write(rocksdb::WriteOptions(), &batch);
            batch.Clear();
        }
    }
    // Flush any remaining records
    if (batch.Count() > 0) {
        db->Write(rocksdb::WriteOptions(), &batch);
    }

    auto insert_end = high_resolution_clock::now();
    double insert_ms = duration_cast<milliseconds>(insert_end - insert_start).count();

    // 🔥 Range Queries
    mt19937 rng(42);
    uniform_int_distribution<int> dist(0, N - RANGE_SIZE - 1);

    auto start = high_resolution_clock::now();
    auto it = db->NewIterator(rocksdb::ReadOptions());

    for (int q = 0; q < QUERIES; q++) {
        int s = dist(rng);
        const string& start_key = dataset[s].first;
        const string& end_key   = dataset[s + RANGE_SIZE].first;

        for (it->Seek(start_key);
             it->Valid() && it->key().compare(end_key) <= 0;
             it->Next()) {
            // Touch the value to ensure it's loaded
            volatile auto val = it->value(); 
        }
    }

    delete it;
    auto end = high_resolution_clock::now();
    double total_ms = duration_cast<milliseconds>(end - start).count();

    // 🧮 Calculate Mathematical Constants
    double omega_lsm = (insert_ms * 1000.0) / N; // microseconds per write
    double total_keys_scanned = QUERIES * RANGE_SIZE;
    double alpha_lsm = (total_ms * 1000.0) / total_keys_scanned; // microseconds per key iterated

    cout << "\n--- RocksDB (Baseline) ---\n";
    cout << "Total Insert Time: " << insert_ms << " ms\n";
    cout << "Total Range Time:  " << total_ms << " ms\n";
    cout << "Cost: ω_LSM = " << omega_lsm << " µs/write\n";
    cout << "Cost: α     = " << alpha_lsm << " µs/key_scanned\n";

    delete db;
}

// ==============================
// LMDB Range Query & Metrics
// ==============================
void lmdb_range() {
    filesystem::remove_all("lmdb_ts");
    filesystem::create_directories("lmdb_ts");

    MDB_env* env;
    MDB_dbi dbi;

    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 2ULL * 1024 * 1024 * 1024); // 2GB Map Size
    mdb_env_open(env, "./lmdb_ts", MDB_NOSYNC, 0664);

    MDB_txn* txn;
    mdb_txn_begin(env, NULL, 0, &txn);
    mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi);

    // 🔥 Measure Insert Time (Chunked Transactions)
    auto insert_start = high_resolution_clock::now();

    for (int i = 0; i < N; i++) {
        MDB_val k, v;
        k.mv_size = dataset[i].first.size();
        k.mv_data = (void*)dataset[i].first.data();
        v.mv_size = dataset[i].second.size();
        v.mv_data = (void*)dataset[i].second.data();

        mdb_put(txn, dbi, &k, &v, 0);

        // Commit and restart transaction every BATCH_SIZE
        if ((i + 1) % BATCH_SIZE == 0) {
            mdb_txn_commit(txn);
            mdb_txn_begin(env, NULL, 0, &txn);
        }
    }
    mdb_txn_commit(txn);

    auto insert_end = high_resolution_clock::now();
    double insert_ms = duration_cast<milliseconds>(insert_end - insert_start).count();

    // 🔥 Range Queries
    mt19937 rng(42);
    uniform_int_distribution<int> dist(0, N - RANGE_SIZE - 1);

    auto start = high_resolution_clock::now();

    MDB_txn* txn_read;
    mdb_txn_begin(env, NULL, MDB_RDONLY, &txn_read);

    MDB_cursor* cursor;
    mdb_cursor_open(txn_read, dbi, &cursor);

    MDB_val k, v;
    for (int q = 0; q < QUERIES; q++) {
        int s = dist(rng);
        const string& start_key = dataset[s].first;
        const string& end_key   = dataset[s + RANGE_SIZE].first;

        k.mv_size = start_key.size();
        k.mv_data = (void*)start_key.data();

        if (mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE) == MDB_SUCCESS) {
            do {
                if (memcmp(k.mv_data, end_key.data(), k.mv_size) > 0)
                    break;
                // Touch the value
                volatile size_t val_size = v.mv_size;
            } while (mdb_cursor_get(cursor, &k, &v, MDB_NEXT) == MDB_SUCCESS);
        }
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn_read);

    auto end = high_resolution_clock::now();
    double total_ms = duration_cast<milliseconds>(end - start).count();

    // 🧮 Calculate Mathematical Constants
    double omega_lmdb = (insert_ms * 1000.0) / N;
    double total_keys_scanned = QUERIES * RANGE_SIZE;
    double alpha_lmdb = (total_ms * 1000.0) / total_keys_scanned;

    cout << "\n--- LMDB (Secondary Index Baseline) ---\n";
    cout << "Total Insert Time: " << insert_ms << " ms\n";
    cout << "Total Range Time:  " << total_ms << " ms\n";
    cout << "Cost: ω_LMDB = " << omega_lmdb << " µs/write\n";
    cout << "Cost: α_LMDB = " << alpha_lmdb << " µs/key_scanned\n\n";

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
}

// ==============================
// MAIN
// ==============================
int main() {
    cout << "Generating 1,000,0000 records...\n";
    generate_data();
    cout << "Data generation complete. Starting benchmarks...\n";

    rocks_range();
    lmdb_range();

    return 0;
}
