# BEARR - Better Efficient Alternative to RocksDB in Rust
<p align="center">
  <img src="img/bearr.png" width="500" />
</p>

BEARR is a high-performance, multithread key-value engine written in Rust.

## Status

All core features are complete.

The following bonus features have also been implemented:
- Handling sequential flooding with 2Q algorithm
- Dostoevsky
- Using a min-heap for Dostoevsky
- Monkey

In addition, our implementation can handle concurrent gets and scans from several threads.

The public interface is fully tested, including an integration test that performs a large number of random database operations, comparing against a HashMap oracle.

Most internal interfaces also have dedicated unit tests.

[TODO: status of experiments]

## How to run

### Tests
You can view the latest test results in github actions via our workflow and the time each test takes plus the maximium memory usage of all tests. 
#### Docker 
To run across all platforms you can use **docker** to run the project. 
```bash
git clone https://github.com/aricasas/BEARR.git
cd BEARR 
docker build -f bearr-test .
docker run bearr-test
```

### In Rust

If you have [Rust and Cargo installed](https://rust-lang.org/tools/install/), you can use Cargo to run tests and experiments.

The basic command is `cargo test` to run all tests in debug mode. To run in release mode, use the `--release` flag. To run while capturing `println` output, provide `-- --nocapture` to the command. To run a specific test or set of tests, provide the name as an argument.

There are three conditional compilation options in the form of Cargo features:
- `binary_search`: use binary search instead of B-tree search for SSTs.
- `uniform_bits`: assign the same number of bits per entry for each LSM level instead of using Monkey.
- `keep_test_files`: do not delete any test files created as a result of running tests.

As an example, to run all the tests in `database.rs` in release mode with `binary_search` and `keep_test_files` active, run the command

```
cargo test --release database --features binary_search,keep_test_files
```

To run the `test_concurrency` test in `database.rs` with `uniform_bits` active and `println` output visible, run the command

```
cargo test database::tests::test_concurrency --features uniform_bits -- --nocapture
```

### Experiments

Instructions for running experiments are in the "Experiments" section of the README.

## Interface

Our project is designed as a library. The main item of interest is the `Database` struct.

Note that this project is Linux-exclusive.

`Database::create(name, configuration)` - creates and returns a database at the given path with the given configuration. Configuration options (`DbConfiguration`) include:
- Options for the LSM tree (`LsmConfiguration`):
  - Size ratio
  - Memtable capacity
  - Number of bits for the bloom filter
- Buffer pool capacity
- Number of pages to buffer for file writes
- Number of pages to buffer for sequential file reads
- Number of operations to buffer for the write-ahead log

`Database::open(name)` - opens and returns an existing database located at the given path.

`database.get(key)` - Returns the value for the given key.

`database.put(key, value)` - inserts the given key-value pair into the database.

`database.delete(key)` - deletes the key-value pair with the given key from the database.

`database.scan(start..=end)` - returns an iterator of key-value pairs where the key is in the given range (start to end inclusive).

`database.flush()` - manually flushes the database, writing the memtable to an SST and writing LSM metadata to disk. The database automatically handles closing upon being dropped, but this function can optionally be called if you need to handle any errors arising from the closing process.

For more detail on the interface, run `cargo doc --open`.

```

## Design

### Public interface

The KV-store APIs are implemented on the `Database` struct in `database.rs`.

Keys and values are of the `u64` type (64-bit unsigned integer). The tombstone value for deletions is represented as `u64::MAX`, and trying to insert this value into the database will return an error.

### LSM tree

LSM trees are implemented as the `LsmTree` struct in `lsm.rs`. They make use of Dostoevsky for compaction, and Monkey (optional, enabled by default) for assigning bloom filter bits.

#### Memtable

#### Merging

### SST and B-tree

SSTs (Sorted String Tables) are immutable files that store key-value data on disk. Each SST consists of four main sections written in the following order:

**Metadata → Leafs → Nodes → Bloom Filter**

---

#### Metadata
The metadata section stores critical information about the SST, including:
- Offsets for each section (leafs, nodes, bloom filter)
- Total SST size
- Bloom filter size and number of hash functions

---

#### Leafs
Leafs are sorted blocks of key-value pairs stored contiguously on disk in the format of Pages. They can be viewed as a persistent, sorted representation of the memtable. Each leaf contains up to 255 key-value pairs (approximately `PAGE_SIZE / (KEY_SIZE + VALUE_SIZE)` = `4096B / 16B` ≈ 255 entries per page).

---

#### Nodes (B+ Tree Index)
The nodes form a B+ tree index over the leafs, enabling efficient lookups. 

**Structure:**
- Each node can reference up to 255 children
- Internal nodes store the maximum key from each child
- The lowest level of nodes points directly to leaf pages
- Nodes are written contiguously to maintain good read/write locality

**Performance Analysis:**

For a 10GB database:
- **Tree depth:** $\log_{255}(\frac{10 \times 10^9}{16}) \approx 4$
- **Index size:** $\frac{10^{10}}{16 \times 255 \times 255} \times 4096 \approx 40\text{MB}$
- **Overhead:** 40MB / 10GB = **0.4%** (negligible)

**Lookup Performance:**
- **With B-tree:** `Tree Depth + 1` I/O accesses (~5 I/Os for 10GB)
- **Without B-tree (binary search):** $\log_2(\frac{\text{DataSize}}{16 \times 255})$ I/O accesses (~21 I/Os for 10GB)

The B-tree index provides approximately **5× faster lookups** compared to binary search. You can disable indexing and use binary search with the `--features binary_search` flag.

**Trade-off:** The B-tree requires sufficient memory to construct the index during SST creation but.

---

#### Bloom Filter
Each SST includes a Bloom filter to quickly determine if a key might exist in the file without reading the entire SST.

**Implementation:**
- Stored as a contiguous byte array after the nodes section
- Size and hash function count are configurable
- Optimizations like Monkey are applied for efficiency

---

#### SST Consistency Guarantees
SSTs maintain consistency through a write-ordering protocol:

**Write Order:** Leafs → Nodes → Bloom Filter → Metadata

The metadata contains a magic number that serves as a consistency check. If any error occurs during the write process, the magic number will be invalid, marking the SST as corrupt and preventing its use. This ensures that partially written SSTs are never treated as valid.

### File system and buffer pool

We have a `FileSystem` struct, implemented in `file_system.rs`, for working with data files. Page IDs -- data of the form (lsm level, sst number, page number) -- are translated into file names by the file system to access files.

The buffer pool, implemented as a hash table, is part of the file system. In order to share the file system in multiple places while simultaneously mutating the buffer pool, we have an inner file system behind a mutex.

Modifying a file invalidates its entries in the buffer pool. To help with this, we have a different page ID type for the buffer pool. Translating between the two ID types is done by the `FileMap` struct, which can disassociate a buffer pool page ID from a regular page ID when the entries in the buffer pool are invalidated.

#### Hashing and hash table

The hash table is implemented in `hashtable.rs` as the `HashTable` struct. It uses linear probing to resolve collisions.

For hash functions, we have a common `HashFunction` struct in `hash.rs` that is used in both the hash table and the bloom filter. It is initialized with a random seed upon creation. The hash algorithm used is MurmurHash.

#### Eviction policy

### Write-Ahead Logging
This database also has configurable write-ahead logging



## Experiments

We designed several experiments to measure the throughput of our get, put, and scan operations. Unless otherwise specified, these were run on the teach.cs server with the following parameters:

- Memtable capacity: 10 MiB
- Buffer pool capacity: 256 MiB
- Bloom filter bits per entry at L1 (using Monkey): 13 bits
- LSM tree size ratio: 4
- Compaction write buffering: 96 pages
- Sequential readahead buffering: 128 pages
- Final database size: 1 GiB
- One sample every 16 MiB of data inserted

We picked the memtable capacity to match what was requested. We picked 256 MiB as the buffer pool capacity to be able to see the difference in our throughputs as the database grows too big to fit in the buffer pool. Our database uses Monkey by default to allocate memory to bloom filters, and we calculated that the total memory used in a 1 GiB database with 8 bits per entry is 512 MiB. To match this total amount of memory, we use 13 bits per entry at the highest LSM tree level, and with Monkey this will end up using a similar amount of total memory.



To run all the experiments and get the CSV data output used to generate the figures, use:
```sh
$ ./run_experiments.sh
```

This will run 25 experiments that build a 1 GiB database each and take a sample of the throughput every 16 MiB inserted. The shortest takes around 2.5 min and the longest around 25 min. In total, they took [TODO].

### Put operation

![](img/put_rolling_avg_throughput.png)

In this experiment we compare the put operation throughput as we vary the size ratio of the LSM tree. We build a 1 GiB database by inserting uniformly random keys and values without any duplicates. We measure the time each put operation takes, and we keep track of the total time spent in put operations. Every 16 MiB of data inserted, we can divide 16 MiB by the time it took to perform the last 16 MiB worth of put operations to calculate the throughput. The data we get from this is really chaotic because compactions happen in some samples but not in others. To make the data easier to interpret and contrast, we calculate a running average of 5 samples and this is what is displayed on the graph.

As we increase the size ratio of the LSM tree, we expect the throughput to increase, which indeed happens. This shows that our database can be tuned to prioritize put operation throughput if the workload is write-heavy.

### Get operation

![](img/get_0pct_throughput.png)

![](img/get_50pct_throughput.png)

![](img/get_100pct_throughput.png)

These

![](img/get_concurrent_throughput.png)

### Scan operation

![](img/scan_concurrent_throughput.png)

In this experiment, we measure the throughput of scan operations in our database.

![](img/full_scan_throughput.png)
