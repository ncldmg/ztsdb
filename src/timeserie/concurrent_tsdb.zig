const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const Allocator = std.mem.Allocator;
const Thread = std.Thread;

const tsdb_mod = @import("tsdb.zig");
const TSDB = tsdb_mod.TSDB;
const lockfree = @import("lockfree_buffer.zig");
const LockFreeBuffer = lockfree.LockFreeBuffer;

pub const DataPoint = tsdb_mod.DataPoint;
pub const SeriesStats = tsdb_mod.SeriesStats;

// High-performance concurrent TSDB with partitioned consumers.
// - Multiple producer threads can insert data lock-free
// - Multiple consumer threads drain partitioned buffers
// - Series are partitioned by ID for parallel processing
pub const ConcurrentTSDB = struct {
    allocator: Allocator,
    tsdb: TSDB,
    tsdb_mutex: Thread.Mutex, // Protects TSDB access (not thread-safe internally)
    partitions: []Partition,
    num_partitions: usize,
    running: std.atomic.Value(bool),
    drain_batch_size: usize,

    const DEFAULT_BUFFER_CAPACITY: usize = 65536;
    const DEFAULT_NUM_PARTITIONS: usize = 4;
    const DEFAULT_DRAIN_BATCH_SIZE: usize = 4096;

    const Partition = struct {
        buffer: LockFreeBuffer,
        consumer_thread: ?Thread,
        drain_batch_size: usize,
        allocator: Allocator,
    };

    pub const Config = struct {
        // TSDB config
        chunk_capacity: usize = TSDB.DEFAULT_CHUNK_CAPACITY,
        wal_segment_size: u64 = TSDB.DEFAULT_WAL_SEGMENT_SIZE,
        batch_size: usize = TSDB.DEFAULT_BATCH_SIZE,
        write_buffer_capacity: usize = TSDB.DEFAULT_WRITE_BUFFER_CAPACITY,
        chunk_pool_size: usize = TSDB.DEFAULT_CHUNK_POOL_SIZE,
        wal_path: []const u8,

        // Tiered storage config
        enable_tiered_storage: bool = false,
        data_dir: ?[]const u8 = null,
        max_warm_chunks: usize = TSDB.DEFAULT_MAX_WARM_CHUNKS,

        // Concurrent ingestion config
        ingestion_buffer_capacity: usize = DEFAULT_BUFFER_CAPACITY,
        num_partitions: usize = DEFAULT_NUM_PARTITIONS,
        drain_batch_size: usize = DEFAULT_DRAIN_BATCH_SIZE,
        drain_interval_ms: u64 = 1,
    };

    pub fn init(allocator: Allocator, config: Config) !ConcurrentTSDB {
        // Initialize underlying TSDB
        const tsdb = try TSDB.init(allocator, .{
            .chunk_capacity = config.chunk_capacity,
            .wal_segment_size = config.wal_segment_size,
            .batch_size = config.batch_size,
            .write_buffer_capacity = config.write_buffer_capacity,
            .chunk_pool_size = config.chunk_pool_size,
            .wal_path = config.wal_path,
            .enable_tiered_storage = config.enable_tiered_storage,
            .data_dir = config.data_dir,
            .max_warm_chunks = config.max_warm_chunks,
        });

        // Calculate per-partition buffer size
        const buffer_per_partition = config.ingestion_buffer_capacity / config.num_partitions;

        // Initialize partitions
        const partitions = try allocator.alloc(Partition, config.num_partitions);
        for (partitions) |*p| {
            p.* = .{
                .buffer = try LockFreeBuffer.init(allocator, buffer_per_partition),
                .consumer_thread = null,
                .drain_batch_size = config.drain_batch_size,
                .allocator = allocator,
            };
        }

        return ConcurrentTSDB{
            .allocator = allocator,
            .tsdb = tsdb,
            .tsdb_mutex = .{},
            .partitions = partitions,
            .num_partitions = config.num_partitions,
            .running = std.atomic.Value(bool).init(false),
            .drain_batch_size = config.drain_batch_size,
        };
    }

    pub fn deinit(self: *ConcurrentTSDB) void {
        self.stop();

        for (self.partitions) |*p| {
            p.buffer.deinit();
        }
        self.allocator.free(self.partitions);
        self.tsdb.deinit();
    }

    // Start consumer threads (one per partition)
    pub fn start(self: *ConcurrentTSDB) !void {
        if (self.running.load(.acquire)) return;
        self.running.store(true, .release);

        for (self.partitions) |*p| {
            p.consumer_thread = try Thread.spawn(.{}, consumerLoop, .{ self, p });
        }
    }

    // Stop all consumer threads and drain remaining data
    pub fn stop(self: *ConcurrentTSDB) void {
        if (!self.running.load(.acquire)) return;
        self.running.store(false, .release);

        // Join all consumer threads
        for (self.partitions) |*p| {
            if (p.consumer_thread) |thread| {
                thread.join();
                p.consumer_thread = null;
            }
        }

        // Final drain of any remaining data
        for (self.partitions) |*p| {
            self.drainPartition(p) catch |err| {
                std.log.err("failed to drain partition on stop: {}", .{err});
            };
        }
    }

    // Insert a data point (lock-free, partitioned by series_id)
    pub fn insert(self: *ConcurrentTSDB, series_id: u64, timestamp: i64, value: f64) bool {
        const partition_idx = series_id % self.num_partitions;
        return self.partitions[partition_idx].buffer.push(.{
            .series_id = series_id,
            .timestamp = timestamp,
            .value = value,
        });
    }

    // Insert returning error on failure
    pub fn insertOrError(self: *ConcurrentTSDB, series_id: u64, timestamp: i64, value: f64) !void {
        if (!self.insert(series_id, timestamp, value)) {
            return error.BufferFull;
        }
    }

    // Insert batch of data points (lock-free)
    // Applies backpressure by spinning briefly if buffer is full
    pub fn insertBatch(self: *ConcurrentTSDB, entries: []const DataPoint) !void {
        for (entries) |entry| {
            const partition_idx = entry.series_id % self.num_partitions;
            const buffer = &self.partitions[partition_idx].buffer;

            // Try with bounded retries to handle transient backpressure
            if (!buffer.pushWait(.{
                .series_id = entry.series_id,
                .timestamp = entry.timestamp,
                .value = entry.value,
            }, 1000)) {
                // If still full after retries, yield to OS scheduler and retry more
                // Use exponential backoff for heavy compaction scenarios
                var attempts: usize = 0;
                var sleep_us: u64 = 100;
                while (attempts < 200) : (attempts += 1) {
                    std.Thread.sleep(sleep_us * std.time.ns_per_us);
                    if (buffer.push(.{
                        .series_id = entry.series_id,
                        .timestamp = entry.timestamp,
                        .value = entry.value,
                    })) break;
                    // Exponential backoff, cap at 10ms
                    sleep_us = @min(sleep_us * 2, 10_000);
                } else {
                    return error.BufferFull;
                }
            }
        }
    }

    // Query data points
    pub fn query(self: *ConcurrentTSDB, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        self.tsdb_mutex.lock();
        defer self.tsdb_mutex.unlock();
        try self.tsdb.query(series_id, start_ts, end_ts, result);
    }

    // Query latest value
    pub fn queryLatest(self: *ConcurrentTSDB, series_id: u64) ?DataPoint {
        self.tsdb_mutex.lock();
        defer self.tsdb_mutex.unlock();
        return self.tsdb.queryLatest(series_id);
    }

    // Force flush all pending data
    // This temporarily stops consumers, drains buffers, then restarts them
    pub fn flush(self: *ConcurrentTSDB) !void {
        const was_running = self.running.load(.acquire);

        if (was_running) {
            // Stop consumer threads
            self.running.store(false, .release);
            for (self.partitions) |*p| {
                if (p.consumer_thread) |thread| {
                    thread.join();
                    p.consumer_thread = null;
                }
            }
        }

        // Now safe to drain (no consumers running)
        {
            self.tsdb_mutex.lock();
            defer self.tsdb_mutex.unlock();
            for (self.partitions) |*p| {
                try self.drainPartition(p);
            }
            try self.tsdb.flush();
        }

        // Restart consumers if they were running
        if (was_running) {
            self.running.store(true, .release);
            for (self.partitions) |*p| {
                p.consumer_thread = try Thread.spawn(.{}, consumerLoop, .{ self, p });
            }
        }
    }

    // Check if running
    pub fn isRunning(self: *const ConcurrentTSDB) bool {
        return self.running.load(.acquire);
    }

    // Sync to disk
    pub fn sync(self: *ConcurrentTSDB) !void {
        self.tsdb_mutex.lock();
        defer self.tsdb_mutex.unlock();
        try self.tsdb.sync();
    }

    // Stats for monitoring
    pub const Stats = struct {
        series_count: u32,
        total_points: u64,
        buffered_points: usize,
    };

    // Get database statistics
    pub fn getStats(self: *ConcurrentTSDB) Stats {
        self.tsdb_mutex.lock();
        defer self.tsdb_mutex.unlock();

        var series_count: u32 = @intCast(self.tsdb.active_chunks.count());
        var total_points: u64 = 0;

        var it = self.tsdb.active_chunks.valueIterator();
        while (it.next()) |chunk_ptr| {
            total_points += chunk_ptr.*.count;
        }
        total_points += self.tsdb.write_buffer.items.len;

        // Count buffered points in partitions (pushed - popped)
        var buffered: usize = 0;
        for (self.partitions) |*p| {
            const pushed = p.buffer.push_count.load(.acquire);
            const popped = p.buffer.pop_count.load(.acquire);
            if (pushed > popped) {
                buffered += @intCast(pushed - popped);
            }
        }

        // Count unique series in write buffer
        var seen = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen.deinit();
        for (self.tsdb.write_buffer.items) |point| {
            if (!self.tsdb.active_chunks.contains(point.series_id)) {
                seen.put(point.series_id, {}) catch continue;
            }
        }
        series_count += @intCast(seen.count());

        return .{
            .series_count = series_count,
            .total_points = total_points,
            .buffered_points = buffered,
        };
    }

    // Consumer thread loop for a single partition
    fn consumerLoop(self: *ConcurrentTSDB, partition: *Partition) void {
        // Allocate drain batch buffer
        const batch = partition.allocator.alloc(lockfree.DataPoint, partition.drain_batch_size) catch {
            std.log.err("failed to allocate drain batch buffer", .{});
            return;
        };
        defer partition.allocator.free(batch);

        // Pre-allocate TSDB batch
        const tsdb_batch = partition.allocator.alloc(DataPoint, partition.drain_batch_size) catch {
            std.log.err("failed to allocate TSDB batch buffer", .{});
            return;
        };
        defer partition.allocator.free(tsdb_batch);

        while (self.running.load(.acquire)) {
            const count = partition.buffer.drain(batch);

            if (count > 0) {
                // Convert to TSDB batch format
                for (batch[0..count], 0..) |p, i| {
                    tsdb_batch[i] = .{
                        .series_id = p.series_id,
                        .timestamp = p.timestamp,
                        .value = p.value,
                    };
                }

                // Batch insert to TSDB (protected by mutex)
                self.tsdb_mutex.lock();
                defer self.tsdb_mutex.unlock();
                self.tsdb.insertBatch(tsdb_batch[0..count]) catch |err| {
                    std.log.err("failed to insert batch: {}", .{err});
                };
            } else {
                // No data, brief sleep
                std.Thread.sleep(100 * std.time.ns_per_us); // 100us
            }
        }
    }

    // Drain a partition's buffer to TSDB
    fn drainPartition(self: *ConcurrentTSDB, partition: *Partition) !void {
        var batch: [4096]lockfree.DataPoint = undefined;
        var tsdb_batch: [4096]DataPoint = undefined;

        while (true) {
            const count = partition.buffer.drain(&batch);
            if (count == 0) break;

            for (batch[0..count], 0..) |p, i| {
                tsdb_batch[i] = .{
                    .series_id = p.series_id,
                    .timestamp = p.timestamp,
                    .value = p.value,
                };
            }

            try self.tsdb.insertBatch(tsdb_batch[0..count]);
        }
    }
};

// Tests

test "ConcurrentTSDB single thread" {
    const allocator = testing.allocator;

    var ctsdb = try ConcurrentTSDB.init(allocator, .{
        .wal_path = "/tmp/test_concurrent_wal.bin",
        .ingestion_buffer_capacity = 1024,
    });
    defer ctsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_concurrent_wal.bin") catch {};

    try ctsdb.start();

    // Insert data
    try testing.expect(ctsdb.insert(1, 1000, 10.0));
    try testing.expect(ctsdb.insert(1, 2000, 20.0));
    try testing.expect(ctsdb.insert(1, 3000, 30.0));

    // Wait for consumer to drain
    std.Thread.sleep(50 * std.time.ns_per_ms);

    // Flush and query
    try ctsdb.flush();

    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try ctsdb.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 3), result.items.len);
}

test "ConcurrentTSDB concurrent producers" {
    const allocator = testing.allocator;

    var ctsdb = try ConcurrentTSDB.init(allocator, .{
        .wal_path = "/tmp/test_concurrent_producers.bin",
        .ingestion_buffer_capacity = 8192,
        .num_partitions = 4,
    });
    defer ctsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_concurrent_producers.bin") catch {};

    try ctsdb.start();

    const num_threads = 4;
    const items_per_thread = 250;

    // Spawn producer threads
    var threads: [num_threads]Thread = undefined;
    for (0..num_threads) |t| {
        threads[t] = try Thread.spawn(.{}, struct {
            fn produce(db: *ConcurrentTSDB, thread_id: usize) void {
                for (0..items_per_thread) |i| {
                    const series_id: u64 = @intCast(thread_id + 1);
                    const timestamp: i64 = @intCast(i * 1000);
                    const value: f64 = @floatFromInt(thread_id * 1000 + i);

                    while (!db.insert(series_id, timestamp, value)) {
                        std.atomic.spinLoopHint();
                    }
                }
            }
        }.produce, .{ &ctsdb, t });
    }

    // Wait for all producers to finish
    for (&threads) |*t| {
        t.join();
    }

    // Wait for consumers to drain
    std.Thread.sleep(100 * std.time.ns_per_ms);
    try ctsdb.flush();

    // Verify all data was inserted
    var total_points: usize = 0;
    for (1..num_threads + 1) |series_id| {
        var result = std.ArrayList(DataPoint){};
        defer result.deinit(allocator);
        try ctsdb.query(@intCast(series_id), 0, std.math.maxInt(i64), &result);
        total_points += result.items.len;
    }

    try testing.expectEqual(num_threads * items_per_thread, total_points);
}
