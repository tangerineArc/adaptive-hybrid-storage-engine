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
#include <sstream>
#include <cstring>

using namespace std;
using namespace std::chrono;

const int N = 1'000'000;
const int RANGE_SIZE = 2000;
const int QUERIES = 1000;

// ==============================
// Encode timestamp (fixed width)
// ==============================
string encode(uint64_t x) {
    stringstream ss;
    ss << setw(16) << setfill('0') << x;
    return ss.str();
}

// ==============================
// Random value
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
        dataset.push_back({encode(i), random_string()});
    }
}

// ==============================
// RocksDB Range Query
// ==============================
void rocks_range() {
    filesystem::remove_all("rocks_ts");

    rocksdb::DB* db;
    rocksdb::Options opt;
    opt.create_if_missing = true;

    rocksdb::DB::Open(opt, "rocks_ts", &db);

    // 🔥 Measure Insert Time
    auto insert_start = high_resolution_clock::now();

    rocksdb::WriteBatch batch;
    for (auto &p : dataset)
        batch.Put(p.first, p.second);

    db->Write(rocksdb::WriteOptions(), &batch);

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
        }
    }

    delete it;

    auto end = high_resolution_clock::now();

    double total_ms = duration_cast<milliseconds>(end - start).count();

    cout << "\n--- RocksDB ---\n";
    cout << "Insert Time: " << insert_ms << " ms\n";
    cout << "Range Total Time: " << total_ms << " ms\n";
    cout << "Avg Range Latency: " << total_ms / QUERIES << " ms/query\n";

    delete db;
}

// ==============================
// LMDB Range Query (FINAL FIXED)
// ==============================
void lmdb_range() {
    filesystem::remove_all("lmdb_ts");
    filesystem::create_directories("lmdb_ts");

    MDB_env* env;
    MDB_dbi dbi;

    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 1ULL << 30);
    mdb_env_open(env, "./lmdb_ts", MDB_NOSYNC, 0664);

    MDB_txn* txn;
    mdb_txn_begin(env, NULL, 0, &txn);
    mdb_dbi_open(txn, NULL, 0, &dbi);

    // 🔥 Measure Insert Time
    auto insert_start = high_resolution_clock::now();

    for (auto &p : dataset) {
        MDB_val k, v;

        k.mv_size = p.first.size();
        k.mv_data = (void*)p.first.data();

        v.mv_size = p.second.size();
        v.mv_data = (void*)p.second.data();

        mdb_put(txn, dbi, &k, &v, 0);
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

            } while (mdb_cursor_get(cursor, &k, &v, MDB_NEXT) == MDB_SUCCESS);
        }
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn_read);

    auto end = high_resolution_clock::now();

    double total_ms = duration_cast<milliseconds>(end - start).count();

    cout << "\n--- LMDB ---\n";
    cout << "Insert Time: " << insert_ms << " ms\n";
    cout << "Range Total Time: " << total_ms << " ms\n";
    cout << "Avg Range Latency: " << total_ms / QUERIES << " ms/query\n";

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
}
// ==============================
// MAIN
// ==============================
int main() {
    generate_data();

    rocks_range();
    lmdb_range();

    return 0;
}
