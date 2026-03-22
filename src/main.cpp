#include "AdaptiveRouter.h"
#include <iostream>

int main() {
  try {
    // This will create a local directory named "db_storage"
    // containing both "rocksdb_data" and "lmdb_data"
    AdaptiveRouter router("./db_storage");

    std::cout << "Inserting data..." << std::endl;
    // 1. Write Data (Goes to RocksDB, background thread picks it up)
    router.Put("user:100", "Alice");
    router.Put("user:105", "Charlie");
    router.Put("user:102", "Bob");    // Inserted out of order
    router.Put("user:108", "David");
    router.Put("user:200", "Eve");    // Outside our scan range

    // 2. Wait a fraction of a second for eventual consistency
    // This gives the background thread time to tail the WAL and update LMDB
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 3. Point Lookup
    std::string val;
    if (router.Get("user:102", val)) {
      std::cout << "GET user:102 -> " << val << "\n";
    }

    // 4. Range Scan (LMDB sorts it, RocksDB fetches values)
    std::cout << "\nScanning from user:100 to user:110...\n";
    auto results = router.Scan("user:100", "user:110");

    for (const auto& pair : results) {
      std::cout << "Found: " << pair.first << " -> " << pair.second << "\n";
    }

  } catch (const std::exception& e) {
    std::cerr << "Initialization failed: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
