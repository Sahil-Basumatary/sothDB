# SothDB

A formally verified database engine written in C++20.

## Building

```
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Running tests

```
cd build
ctest --output-on-failure
```
