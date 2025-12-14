# ZTSDB Optimization TODO

High-frequency time series data optimizations.

## Write Path

- [x] **Buffer writes in memory before WAL**
  - Add `write_buffer: ArrayList(DataPoint)` to TSDB
  - Configurable `write_buffer_capacity` (default 1000)
  - Flush buffer to WAL + chunks in single operation
  - Reduces syscalls and io_uring submissions
  - Query functions check both chunks and write buffer
  - Added `insertImmediate()` for critical data that bypasses buffer

- [x] **Batch io_uring submissions**
  - `write()` queues but doesn't submit
  - `submit()` sends all queued ops to kernel (non-blocking)
  - `flush()` submits and waits for all completions
  - Track `pending_writes` for proper completion handling
  - Result: 1 submit per flush (1000+ writes batched)

- [ ] **Pre-allocate chunk pool**
  - Avoid allocations during hot ingestion path
  - Pool of ready-to-use chunks
  - Reset and reuse instead of alloc/free

- [ ] **Lock-free ingestion buffer**
  - Atomic ring buffer for concurrent writers
  - Multiple producer threads, single consumer
  - Avoid mutex contention on hot path

## Compression

- [ ] **Delta encoding for timestamps**
  - Store `base_timestamp` + array of deltas
  - High-frequency data: deltas fit in i16 (2 bytes vs 8)
  - Expected compression: 4x for timestamps

- [ ] **Gorilla compression for values**
  - XOR consecutive f64 values
  - Most bits unchanged between samples
  - Facebook paper: 12x compression typical
  - Implementation: leading zeros + meaningful bits encoding

- [ ] **Run-length encoding for repeated values**
  - Common in sensor data (value unchanged)
  - Store (value, count) pairs

## Storage Tiers

- [ ] **Hot tier: in-memory chunks**
  - Recent data, fast writes
  - Current active chunks

- [ ] **Warm tier: mmap'd chunks**
  - Completed chunks, memory-mapped
  - Zero-copy queries
  - OS manages page cache

- [ ] **Cold tier: compressed files**
  - Old data, compressed on disk
  - Load on demand for historical queries

## Query Path

- [ ] **Timestamp index**
  - Skip list or B-tree for range queries
  - O(log n) to find start of range
  - Current: O(n) scan

- [ ] **Bloom filters per chunk**
  - Quick "series not in chunk" check
  - Avoid scanning irrelevant chunks

- [ ] **Query result streaming**
  - Don't load all results in memory
  - Iterator-based API

## Concurrency

- [ ] **Per-series locks**
  - Current: global lock (implicit)
  - Target: lock per series_id
  - Allows parallel writes to different series

- [ ] **Read-write locks for chunks**
  - Multiple concurrent readers
  - Exclusive writer access

## Monitoring

- [ ] **Metrics collection**
  - Writes/sec, queries/sec
  - WAL size, chunk count
  - Latency percentiles (p50, p99)

- [ ] **Expose via protocol**
  - Add `stats` message type
  - Return TSDB metrics

## Build & Test

- [ ] **Benchmark suite**
  - Ingestion throughput (points/sec)
  - Query latency
  - Compression ratios

- [ ] **Fuzz testing**
  - Protocol fuzzing
  - Crash recovery testing

## Priority Order

1. ~~Buffer writes in memory~~ (DONE)
2. ~~Batch io_uring submissions~~ (DONE)
3. Delta encoding for timestamps
4. Gorilla compression for values
5. Pre-allocate chunk pool
6. Timestamp index for queries
