#include <rocksdb/db.h>
#include <iostream>

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::Status status = rocksdb::DB::Open(options, "testdb", &db);

    if (!status.ok()) {
        std::cout << "DB open failed\n";
        return 1;
    }

    db->Put(rocksdb::WriteOptions(), "100", "hello");
    db->Put(rocksdb::WriteOptions(), "200", "world");

    std::string value;
    db->Get(rocksdb::ReadOptions(), "100", &value);
    std::cout << "Key 100: " << value << std::endl;

    // Range query
    auto it = db->NewIterator(rocksdb::ReadOptions());
    for (it->Seek("100"); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << " -> "
                  << it->value().ToString() << std::endl;
    }

    delete it;
    delete db;
}
