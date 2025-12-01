# BEARR - Better Efficient Alternative to RocksDB in Rust
<p align="center">
  <img src="img/bearr.png" width="500" />
</p>

BEARR is a key-value store written in Rust as our CSC443 course project.

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

## How to run

[TODO]

## Design

### LSM tree

#### Merging

### SST and B-tree

#### Bloom filter

### File system and buffer pool

#### Hashing and hash table

#### Eviction policy

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
