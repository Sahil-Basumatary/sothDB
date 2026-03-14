# SothDB

Disk-oriented storage engine in C++20. Building the full stack from page management up through query execution and no third-party storage libraries.

Kind of inspired by how systems like InnoDB and WiredTiger are structured, with design decisions drawn from the [CMU 15-445](https://15445.courses.cs.cmu.edu/) curriculum and the Stonebraker/Hellerstein architecture.

## Architecture

```
SQL Layer        → parser, binder, planner/optimizer (planned)
Execution        → volcano-style iterator model (planned)
Access Methods   → B+ tree, sequential scan (planned)
Buffer Pool      → page cache, LRU eviction ✓
Disk Manager     → page-level I/O to a single db file ✓
```

Storage is page-oriented — 4 KB fixed-size pages with a slotted layout for variable-length tuples. The buffer pool sits between the execution layer and disk, managing pin counts and dirty page tracking.

### Current status

- **Disk Manager** — page read/write against a single file, monotonic page allocation (no free-list as of now)
- **Slotted Pages** — insert, get, delete with slot directory; no compaction yet
- **LRU Replacer** — eviction tracking for unpinned frames
- **Buffer Pool** — fetch, new, unpin, flush, delete; thread-safe with a single latch

## Build

Requires C++20 (GCC 11+, Clang 14+) and CMake 3.20+.

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

On macOS use `make -j$(sysctl -n hw.ncpu)` or just `make -j8`.

## Tests

```bash
cd build
ctest --output-on-failure
```
