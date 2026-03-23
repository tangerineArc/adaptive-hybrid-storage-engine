cmake_minimum_required(VERSION 3.10)
project(AdaptiveIndex)

# Enforce C++17 (RocksDB and modern C++ features require this)
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

# ==========================================
# 1. Locate RocksDB
# ==========================================
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
find_library(ROCKSDB_LIBRARY NAMES rocksdb)

if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
  message(FATAL_ERROR "RocksDB not found.\n"
                      "-> On Ubuntu/Debian run: sudo apt install librocksdb-dev\n"
                      "-> On Fedora/RHEL run:   sudo dnf install rocksdb-devel")
endif()

# ==========================================
# 2. Locate LMDB
# ==========================================
find_path(LMDB_INCLUDE_DIR NAMES lmdb.h)
find_library(LMDB_LIBRARY NAMES lmdb)

if(NOT LMDB_INCLUDE_DIR OR NOT LMDB_LIBRARY)
  message(FATAL_ERROR "LMDB not found.\n"
                      "-> On Ubuntu/Debian run: sudo apt install liblmdb-dev\n"
                      "-> On Fedora/RHEL run:   sudo dnf install lmdb-devel")
endif()

# ==========================================
# 3. Define the Executable Targets
# ==========================================
# Main Application Target
add_executable(adaptive_router
  src/main.cpp
  src/AdaptiveRouter.cpp
)

# Benchmark Application Target
add_executable(adaptive_benchmark
  src/benchmark.cpp
  src/AdaptiveRouter.cpp
)

# ==========================================
# 4. Include Headers and Link Libraries
# ==========================================
# --- Configure adaptive_router ---
target_include_directories(adaptive_router PRIVATE
  ${ROCKSDB_INCLUDE_DIR}
  ${LMDB_INCLUDE_DIR}
)

target_link_libraries(adaptive_router
  ${ROCKSDB_LIBRARY}
  ${LMDB_LIBRARY}
  Threads::Threads
)

# --- Configure adaptive_benchmark ---
target_include_directories(adaptive_benchmark PRIVATE
  ${ROCKSDB_INCLUDE_DIR}
  ${LMDB_INCLUDE_DIR}
)

target_link_libraries(adaptive_benchmark
  ${ROCKSDB_LIBRARY}
  ${LMDB_LIBRARY}
  Threads::Threads
)

# Add the raw comparison benchmark
add_executable(raw_compare_bench src/raw_compare_bench.cpp)
target_link_libraries(raw_compare_bench ${ROCKSDB_LIBRARY} ${LMDB_LIBRARY} Threads::Threads)

# Add the sequential comparison benchmark
add_executable(sequential_bench src/sequential_bench.cpp)
target_link_libraries(sequential_bench ${ROCKSDB_LIBRARY} ${LMDB_LIBRARY} Threads::Threads)
