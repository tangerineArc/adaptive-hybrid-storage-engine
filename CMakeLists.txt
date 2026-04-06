cmake_minimum_required(VERSION 3.10)
project(AdaptiveIndex)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Disable RTTI to match RocksDB's default pre-compiled binaries
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")

# Default to Release build for optimized performance
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Find the system's standard threading library (Cross-platform pthread)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

# Locate Abseil
find_package(absl REQUIRED)

# Locate RocksDB
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
find_library(ROCKSDB_LIBRARY NAMES rocksdb)

if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
  message(FATAL_ERROR "RocksDB not found.\n"
                      "-> On Ubuntu/Debian run: sudo apt install librocksdb-dev\n"
                      "-> On Fedora/RHEL run:   sudo dnf install rocksdb-devel")
endif()

# Locate LMDB
find_path(LMDB_INCLUDE_DIR NAMES lmdb.h)
find_library(LMDB_LIBRARY NAMES lmdb)

if(NOT LMDB_INCLUDE_DIR OR NOT LMDB_LIBRARY)
  message(FATAL_ERROR "LMDB not found.\n"
                      "-> On Ubuntu/Debian run: sudo apt install liblmdb-dev\n"
                      "-> On Fedora/RHEL run:   sudo dnf install lmdb-devel")
endif()

add_executable(stress_test src/main.cpp)
target_link_libraries(stress_test ${ROCKSDB_LIBRARY} ${LMDB_LIBRARY} absl::btree Threads::Threads)
