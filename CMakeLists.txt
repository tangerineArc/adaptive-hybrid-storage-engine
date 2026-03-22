cmake_minimum_required(VERSION 3.10)
project(AdaptiveIndex)

# Enforce C++17 (RocksDB and modern C++ features require this)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ==========================================
# 1. Locate RocksDB
# ==========================================
# Searches /usr/include and /usr/lib64 (standard paths in linux distros)
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
find_library(ROCKSDB_LIBRARY NAMES rocksdb)

if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
  message(FATAL_ERROR "RocksDB not found. Please ensure 'rocksdb-devel' is installed.")
endif()

# ==========================================
# 2. Locate LMDB
# ==========================================
find_path(LMDB_INCLUDE_DIR NAMES lmdb.h)
find_library(LMDB_LIBRARY NAMES lmdb)

if(NOT LMDB_INCLUDE_DIR OR NOT LMDB_LIBRARY)
  message(FATAL_ERROR "LMDB not found. Please ensure 'lmdb-devel' is installed.")
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
  pthread
)

# --- Configure adaptive_benchmark ---
target_include_directories(adaptive_benchmark PRIVATE
  ${ROCKSDB_INCLUDE_DIR}
  ${LMDB_INCLUDE_DIR}
)

target_link_libraries(adaptive_benchmark
  ${ROCKSDB_LIBRARY}
  ${LMDB_LIBRARY}
  pthread
)
