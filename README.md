## How to install development dependencies?
> The below installation instruction is for Fedora GNU/Linux distribution. For your particular OS, use a search engine or LLM to look up corresponding instructions.
```sh
sudo dnf install rocksdb-devel lmdb-devel cmake gcc g++
```

## How to compile and run?
Navigate to your project's root directory in the terminal and execute the standard out-of-source CMake build process:
```sh
mkdir build
cd build
cmake ..
make
```
If everything compiles successfully, run your new binary:
```sh
./adaptive-router
```
