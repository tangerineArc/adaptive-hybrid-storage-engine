#include <lmdb.h>
#include <iostream>

int main() {
    MDB_env* env;

    if (mdb_env_create(&env) != 0) {
        std::cout << "LMDB init failed\n";
        return 1;
    }

    std::cout << "LMDB working\n";

    mdb_env_close(env);
    return 0;
}
