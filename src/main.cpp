#include "AdaptiveRouter.h"
#include <iostream>

int main() {
  try {
    // This will create a local directory named "db_storage"
    // containing both "rocksdb_data" and "lmdb_data"
    AdaptiveRouter router("./db_storage");

    std::cout << "Inserting data..." << std::endl;
    router.Put("user:1001", "Alice");
    router.Put("user:1002", "Bob");
    router.Delete("user:1001");

    std::cout << "Write path executed successfully!" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Initialization failed: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
