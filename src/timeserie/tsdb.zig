const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const testing = std.testing;
const Allocator = std.mem.Allocator;
const Thread = std.Thread;

const Chunk = @import("chunk.zig").Chunk;
const ChunkPool = @import("chunk.zig").ChunkPool;
const WAL = @import("wal.zig").WAL;
const tiered_mod = @import("tiered_storage.zig");
const TieredStorage = tiered_mod.TieredStorage;
const ColdStorage = @import("cold_storage.zig").ColdStorage;
const MmapChunk = @import("mmap_chunk.zig").MmapChunk;
const LockFreeBuffer = @import("lockfree_buffer.zig").LockFreeBuffer;

pub const DataPoint = @import("../protocol/protocol.zig").DataPoint;

pub const SeriesStats = struct {
    count: usize,
    min_value: f64,
    max_value: f64,
    avg_value: f64,
    min_timestamp: i64,
    max_timestamp: i64,
};

// Partition for concurrent ingestion.
// Each partition owns its own chunk map and lock-free buffer.
const Partition = struct {
    buffer: LockFreeBuffer,
    chunks: std.AutoHashMap(u64, *Chunk),
    chunk_pool: ChunkPool,
    consumer_thread: ?Thread,
    chunk_map_mutex: Thread.Mutex,
    allocator: Allocator,
    drain_batch_size: usize,

    fn init(allocator: Allocator, buffer_capacity: usize, chunk_capacity: usize, pool_size: usize, drain_batch_size: usize) !Partition {
        return Partition{
            .buffer = try LockFreeBuffer.init(allocator, buffer_capacity),
            .chunks = std.AutoHashMap(u64, *Chunk).init(allocator),
            .chunk_pool = try ChunkPool.init(allocator, chunk_capacity, pool_size),
            .consumer_thread = null,
            .chunk_map_mutex = .{},
            .allocator = allocator,
            .drain_batch_size = drain_batch_size,
        };
    }

    fn deinit(self: *Partition) void {
        // Release all chunks back to pool
        var it = self.chunks.valueIterator();
        while (it.next()) |chunk_ptr| {
            self.chunk_pool.release(chunk_ptr.*);
        }
        self.chunks.deinit();
        self.chunk_pool.deinit();
        self.buffer.deinit();
    }
};

/// Unified Time Series Database with optional concurrent ingestion.
///
/// When `concurrent_mode = true` (default):
/// - Lock-free partitioned buffers for high-throughput ingestion
/// - Consumer threads drain buffers directly to partition-local chunks
/// - No double-buffering overhead
///
/// When `concurrent_mode = false`:
/// - Direct single-threaded chunk access
/// - No concurrency overhead
/// - Suitable for embedded or single-threaded use cases
pub const TSDB = struct {
    allocator: Allocator,

    // Concurrency state
    concurrent_mode: bool,
    running: std.atomic.Value(bool),

    // Ingestion layer (null if concurrent_mode = false)
    partitions: ?[]Partition,
    num_partitions: usize,

    // Storage layer for single-threaded mode
    global_chunks: ?std.AutoHashMap(u64, *Chunk),
    global_chunk_pool: ?ChunkPool,

    // WAL for durability
    wal: WAL,
    wal_serialize_buffer: std.ArrayList(u8),

    // Optional tiered storage
    tiered_storage: ?TieredStorage,
    data_dir: ?fs.Dir,

    // Config values
    chunk_capacity: usize,
    drain_batch_size: usize,

    // Constants
    pub const DEFAULT_CHUNK_CAPACITY: usize = 8192;
    pub const DEFAULT_WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_CHUNK_POOL_SIZE: usize = 16;
    pub const DEFAULT_MAX_WARM_CHUNKS: usize = 100;
    pub const DEFAULT_NUM_PARTITIONS: usize = 4;
    pub const DEFAULT_BUFFER_CAPACITY: usize = 65536;
    pub const DEFAULT_DRAIN_BATCH_SIZE: usize = 4096;

    pub const Config = struct {
        // Storage config
        chunk_capacity: usize = DEFAULT_CHUNK_CAPACITY,
        wal_segment_size: u64 = DEFAULT_WAL_SEGMENT_SIZE,
        chunk_pool_size: usize = DEFAULT_CHUNK_POOL_SIZE,
        wal_path: []const u8,

        // Tiered storage config
        enable_tiered_storage: bool = false,
        data_dir: ?[]const u8 = null,
        max_warm_chunks: usize = DEFAULT_MAX_WARM_CHUNKS,

        // Concurrency config
        concurrent_mode: bool = true,
        num_partitions: usize = DEFAULT_NUM_PARTITIONS,
        ingestion_buffer_capacity: usize = DEFAULT_BUFFER_CAPACITY,
        drain_batch_size: usize = DEFAULT_DRAIN_BATCH_SIZE,
    };

    pub fn init(allocator: Allocator, config: Config) !TSDB {
        const cwd = fs.cwd();
        const wal = try WAL.init(allocator, config.wal_segment_size, cwd, config.wal_path);

        // Optional tiered storage
        var tiered_storage: ?TieredStorage = null;
        var data_dir: ?fs.Dir = null;

        if (config.enable_tiered_storage) {
            if (config.data_dir) |dir_path| {
                cwd.makeDir(dir_path) catch |err| {
                    if (err != error.PathAlreadyExists) return err;
                };
                data_dir = try cwd.openDir(dir_path, .{});
                tiered_storage = try TieredStorage.init(allocator, data_dir.?, .{
                    .chunk_capacity = config.chunk_capacity,
                    .chunk_pool_size = config.chunk_pool_size,
                    .max_warm_chunks = config.max_warm_chunks,
                });
            }
        }

        var tsdb = TSDB{
            .allocator = allocator,
            .concurrent_mode = config.concurrent_mode,
            .running = std.atomic.Value(bool).init(false),
            .partitions = null,
            .num_partitions = config.num_partitions,
            .global_chunks = null,
            .global_chunk_pool = null,
            .wal = wal,
            .wal_serialize_buffer = std.ArrayList(u8){},
            .tiered_storage = tiered_storage,
            .data_dir = data_dir,
            .chunk_capacity = config.chunk_capacity,
            .drain_batch_size = config.drain_batch_size,
        };

        if (config.concurrent_mode) {
            // Initialize partitioned ingestion layer
            const buffer_per_partition = config.ingestion_buffer_capacity / config.num_partitions;
            const pool_per_partition = @max(config.chunk_pool_size / config.num_partitions, 2);

            const partitions = try allocator.alloc(Partition, config.num_partitions);
            errdefer allocator.free(partitions);

            var initialized: usize = 0;
            errdefer {
                for (partitions[0..initialized]) |*p| {
                    p.deinit();
                }
            }

            for (partitions) |*p| {
                p.* = try Partition.init(
                    allocator,
                    buffer_per_partition,
                    config.chunk_capacity,
                    pool_per_partition,
                    config.drain_batch_size,
                );
                initialized += 1;
            }

            tsdb.partitions = partitions;
        } else {
            // Initialize single-threaded storage
            tsdb.global_chunks = std.AutoHashMap(u64, *Chunk).init(allocator);
            tsdb.global_chunk_pool = try ChunkPool.init(
                allocator,
                config.chunk_capacity,
                config.chunk_pool_size,
            );
        }

        return tsdb;
    }

    pub fn deinit(self: *TSDB) void {
        self.stop();

        if (self.concurrent_mode) {
            if (self.partitions) |partitions| {
                for (partitions) |*p| {
                    p.deinit();
                }
                self.allocator.free(partitions);
            }
        } else {
            if (self.global_chunks) |*chunks| {
                var it = chunks.valueIterator();
                while (it.next()) |chunk_ptr| {
                    self.global_chunk_pool.?.release(chunk_ptr.*);
                }
                chunks.deinit();
            }
            if (self.global_chunk_pool) |*pool| {
                pool.deinit();
            }
        }

        if (self.tiered_storage) |*ts| {
            ts.deinit();
        }
        if (self.data_dir) |dir| {
            var d = dir;
            d.close();
        }

        self.wal_serialize_buffer.deinit(self.allocator);
        self.wal.deinit();
    }

    // ============ Lifecycle ============

    /// Start consumer threads (no-op if concurrent_mode = false)
    pub fn start(self: *TSDB) !void {
        if (!self.concurrent_mode) return;
        if (self.running.load(.acquire)) return;

        self.running.store(true, .release);

        if (self.partitions) |partitions| {
            for (partitions) |*p| {
                p.consumer_thread = try Thread.spawn(.{}, consumerLoop, .{ self, p });
            }
        }
    }

    /// Stop consumer threads and drain remaining data (no-op if concurrent_mode = false)
    pub fn stop(self: *TSDB) void {
        if (!self.concurrent_mode) return;
        if (!self.running.load(.acquire)) return;

        self.running.store(false, .release);

        // Join all consumer threads
        if (self.partitions) |partitions| {
            for (partitions) |*p| {
                if (p.consumer_thread) |thread| {
                    thread.join();
                    p.consumer_thread = null;
                }
            }

            // Final drain
            for (partitions) |*p| {
                self.drainPartition(p) catch |err| {
                    std.log.err("failed to drain partition on stop: {}", .{err});
                };
            }
        }
    }

    /// Check if consumer threads are running
    pub fn isRunning(self: *const TSDB) bool {
        if (!self.concurrent_mode) return true;
        return self.running.load(.acquire);
    }

    // ============ Insert Operations ============

    /// Insert a data point. Returns true on success, false if buffer full.
    /// In concurrent mode, uses lock-free partitioned buffers.
    /// In single-threaded mode, writes directly to chunks.
    pub fn insert(self: *TSDB, series_id: u64, timestamp: i64, value: f64) bool {
        if (self.concurrent_mode) {
            const partition_idx = series_id % self.num_partitions;
            return self.partitions.?[partition_idx].buffer.push(.{
                .series_id = series_id,
                .timestamp = timestamp,
                .value = value,
            });
        } else {
            self.insertDirect(series_id, timestamp, value) catch return false;
            return true;
        }
    }

    /// Insert with error on failure
    pub fn insertOrError(self: *TSDB, series_id: u64, timestamp: i64, value: f64) !void {
        if (!self.insert(series_id, timestamp, value)) {
            return error.BufferFull;
        }
    }

    /// Insert batch with backpressure handling
    pub fn insertBatch(self: *TSDB, entries: []const DataPoint) !void {
        if (entries.len == 0) return;

        if (self.concurrent_mode) {
            for (entries) |entry| {
                const partition_idx = entry.series_id % self.num_partitions;
                const buffer = &self.partitions.?[partition_idx].buffer;

                if (!buffer.pushWait(.{
                    .series_id = entry.series_id,
                    .timestamp = entry.timestamp,
                    .value = entry.value,
                }, 1000)) {
                    // Exponential backoff for heavy load
                    var attempts: usize = 0;
                    var sleep_us: u64 = 100;
                    while (attempts < 200) : (attempts += 1) {
                        Thread.sleep(sleep_us * std.time.ns_per_us);
                        if (buffer.push(.{
                            .series_id = entry.series_id,
                            .timestamp = entry.timestamp,
                            .value = entry.value,
                        })) break;
                        sleep_us = @min(sleep_us * 2, 10_000);
                    } else {
                        return error.BufferFull;
                    }
                }
            }
        } else {
            for (entries) |entry| {
                try self.insertDirect(entry.series_id, entry.timestamp, entry.value);
            }
        }
    }

    /// Direct insert bypassing lock-free buffer (single-threaded mode or internal use)
    fn insertDirect(self: *TSDB, series_id: u64, timestamp: i64, value: f64) !void {
        const chunk = try self.getOrCreateGlobalChunk(series_id);
        try chunk.insert(timestamp, value);

        if (chunk.isFull()) {
            try self.flushChunkToWarm(chunk);
            chunk.reset(series_id);
        }
    }

    // ============ Query Operations ============

    /// Query data points for a series within a time range
    pub fn query(self: *TSDB, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        if (self.concurrent_mode) {
            const partition_idx = series_id % self.num_partitions;
            const partition = &self.partitions.?[partition_idx];

            // Query partition-local chunks
            partition.chunk_map_mutex.lock();
            defer partition.chunk_map_mutex.unlock();

            if (partition.chunks.get(series_id)) |chunk| {
                try self.queryChunk(chunk, series_id, start_ts, end_ts, result);
            }

            // Query unflushed data in lock-free buffer
            try self.queryLockFreeBuffer(&partition.buffer, series_id, start_ts, end_ts, result);
        } else {
            // Single-threaded: query global chunks
            if (self.global_chunks.?.get(series_id)) |chunk| {
                try self.queryChunk(chunk, series_id, start_ts, end_ts, result);
            }
        }

        // Query tiered storage
        if (self.tiered_storage) |*ts| {
            var tiered_result = std.ArrayList(tiered_mod.DataPoint){};
            defer tiered_result.deinit(self.allocator);

            try ts.query(series_id, start_ts, end_ts, &tiered_result);

            for (tiered_result.items) |p| {
                try result.append(self.allocator, .{
                    .series_id = p.series_id,
                    .timestamp = p.timestamp,
                    .value = p.value,
                });
            }
        }
    }

    /// Query the latest value for a series
    pub fn queryLatest(self: *TSDB, series_id: u64) ?DataPoint {
        var latest: ?DataPoint = null;

        if (self.concurrent_mode) {
            const partition_idx = series_id % self.num_partitions;
            const partition = &self.partitions.?[partition_idx];

            partition.chunk_map_mutex.lock();
            defer partition.chunk_map_mutex.unlock();

            if (partition.chunks.get(series_id)) |chunk| {
                if (chunk.count > 0) {
                    const idx = chunk.count - 1;
                    latest = DataPoint{
                        .series_id = series_id,
                        .timestamp = chunk.timestamps[idx],
                        .value = chunk.values[idx],
                    };
                }
            }

            // Note: Cannot efficiently check lock-free buffer for latest
            // without draining it. The chunk contains the most recent flushed data.
        } else {
            if (self.global_chunks.?.get(series_id)) |chunk| {
                if (chunk.count > 0) {
                    const idx = chunk.count - 1;
                    latest = DataPoint{
                        .series_id = series_id,
                        .timestamp = chunk.timestamps[idx],
                        .value = chunk.values[idx],
                    };
                }
            }
        }

        return latest;
    }

    // ============ Maintenance ============

    /// Force flush all pending data
    pub fn flush(self: *TSDB) !void {
        if (self.concurrent_mode) {
            const was_running = self.running.load(.acquire);

            if (was_running) {
                // Stop consumer threads
                self.running.store(false, .release);
                if (self.partitions) |partitions| {
                    for (partitions) |*p| {
                        if (p.consumer_thread) |thread| {
                            thread.join();
                            p.consumer_thread = null;
                        }
                    }
                }
            }

            // Drain all partitions
            if (self.partitions) |partitions| {
                for (partitions) |*p| {
                    try self.drainPartition(p);
                }
            }

            // Restart consumers if they were running
            if (was_running) {
                self.running.store(true, .release);
                if (self.partitions) |partitions| {
                    for (partitions) |*p| {
                        p.consumer_thread = try Thread.spawn(.{}, consumerLoop, .{ self, p });
                    }
                }
            }
        }

        try self.wal.flush();
    }

    /// Sync WAL to disk for durability
    pub fn sync(self: *TSDB) !void {
        try self.flush();
        try self.wal.sync();
    }

    /// Run maintenance tasks (compact warm to cold, etc.)
    pub fn runMaintenance(self: *TSDB) !void {
        if (self.tiered_storage) |*ts| {
            try ts.runMaintenance();
        }
    }

    /// Check if tiered storage is enabled
    pub fn hasTieredStorage(self: *const TSDB) bool {
        return self.tiered_storage != null;
    }

    /// Get tiered storage stats
    pub fn getTieredStats(self: *const TSDB) ?TieredStorage.Stats {
        if (self.tiered_storage) |ts| {
            return ts.getStats();
        }
        return null;
    }

    // ============ Statistics ============

    pub const Stats = struct {
        series_count: u32,
        total_points: u64,
        buffered_points: usize,
    };

    /// Get database statistics
    pub fn getStats(self: *TSDB) Stats {
        var series_count: u32 = 0;
        var total_points: u64 = 0;
        var buffered_points: usize = 0;

        if (self.concurrent_mode) {
            if (self.partitions) |partitions| {
                for (partitions) |*p| {
                    p.chunk_map_mutex.lock();
                    defer p.chunk_map_mutex.unlock();

                    series_count += @intCast(p.chunks.count());

                    var it = p.chunks.valueIterator();
                    while (it.next()) |chunk_ptr| {
                        total_points += chunk_ptr.*.count;
                    }

                    // Count buffered points
                    const pushed = p.buffer.push_count.load(.acquire);
                    const popped = p.buffer.pop_count.load(.acquire);
                    if (pushed > popped) {
                        buffered_points += @intCast(pushed - popped);
                    }
                }
            }
        } else {
            if (self.global_chunks) |chunks| {
                series_count = @intCast(chunks.count());

                var it = chunks.valueIterator();
                while (it.next()) |chunk_ptr| {
                    total_points += chunk_ptr.*.count;
                }
            }
        }

        return .{
            .series_count = series_count,
            .total_points = total_points,
            .buffered_points = buffered_points,
        };
    }

    /// Get statistics for a specific series
    pub fn getSeriesStats(self: *TSDB, series_id: u64) ?SeriesStats {
        var chunk: ?*Chunk = null;

        if (self.concurrent_mode) {
            const partition_idx = series_id % self.num_partitions;
            const partition = &self.partitions.?[partition_idx];

            partition.chunk_map_mutex.lock();
            defer partition.chunk_map_mutex.unlock();

            chunk = partition.chunks.get(series_id);
        } else {
            chunk = self.global_chunks.?.get(series_id);
        }

        if (chunk == null or chunk.?.count == 0) return null;

        const c = chunk.?;
        var min_val: f64 = std.math.inf(f64);
        var max_val: f64 = -std.math.inf(f64);
        var sum: f64 = 0;

        for (c.values[0..c.count]) |val| {
            if (val < min_val) min_val = val;
            if (val > max_val) max_val = val;
            sum += val;
        }

        return SeriesStats{
            .count = c.count,
            .min_value = min_val,
            .max_value = max_val,
            .avg_value = sum / @as(f64, @floatFromInt(c.count)),
            .min_timestamp = c.min_ts,
            .max_timestamp = c.max_ts,
        };
    }

    // ============ Internal: Consumer Loop ============

    fn consumerLoop(self: *TSDB, partition: *Partition) void {
        const batch = partition.allocator.alloc(DataPoint, partition.drain_batch_size) catch {
            std.log.err("failed to allocate drain batch buffer", .{});
            return;
        };
        defer partition.allocator.free(batch);

        while (self.running.load(.acquire)) {
            const count = partition.buffer.drain(batch);

            if (count > 0) {
                self.insertBatchToPartition(partition, batch[0..count]) catch |err| {
                    std.log.err("consumer insert failed: {}", .{err});
                };
            } else {
                Thread.sleep(100 * std.time.ns_per_us);
            }
        }
    }

    fn insertBatchToPartition(self: *TSDB, partition: *Partition, entries: []const DataPoint) !void {
        for (entries) |entry| {
            const chunk = try self.getOrCreatePartitionChunk(partition, entry.series_id);
            try chunk.insert(entry.timestamp, entry.value);

            if (chunk.isFull()) {
                try self.flushChunkToWarm(chunk);
                chunk.reset(entry.series_id);
            }
        }
    }

    fn drainPartition(self: *TSDB, partition: *Partition) !void {
        var batch: [4096]DataPoint = undefined;

        while (true) {
            const count = partition.buffer.drain(&batch);
            if (count == 0) break;

            try self.insertBatchToPartition(partition, batch[0..count]);
        }
    }

    // ============ Internal: Chunk Management ============

    fn getOrCreatePartitionChunk(_: *TSDB, partition: *Partition, series_id: u64) !*Chunk {
        partition.chunk_map_mutex.lock();
        defer partition.chunk_map_mutex.unlock();

        const entry = try partition.chunks.getOrPut(series_id);

        if (!entry.found_existing) {
            const chunk_ptr = try partition.chunk_pool.acquire(series_id);
            entry.value_ptr.* = chunk_ptr;
        }

        return entry.value_ptr.*;
    }

    fn getOrCreateGlobalChunk(self: *TSDB, series_id: u64) !*Chunk {
        const entry = try self.global_chunks.?.getOrPut(series_id);

        if (!entry.found_existing) {
            const chunk_ptr = try self.global_chunk_pool.?.acquire(series_id);
            entry.value_ptr.* = chunk_ptr;
        }

        const chunk = entry.value_ptr.*;

        if (chunk.isFull()) {
            try self.flushChunkToWarm(chunk);
            chunk.reset(series_id);
        }

        return chunk;
    }

    fn queryChunk(self: *TSDB, chunk: *Chunk, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        if (chunk.count == 0) return;
        if (end_ts < chunk.min_ts or start_ts > chunk.max_ts) return;

        for (chunk.timestamps[0..chunk.count], chunk.values[0..chunk.count]) |ts, val| {
            if (ts >= start_ts and ts <= end_ts) {
                try result.append(self.allocator, .{
                    .series_id = series_id,
                    .timestamp = ts,
                    .value = val,
                });
            }
        }
    }

    fn queryLockFreeBuffer(self: *TSDB, buffer: *LockFreeBuffer, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        // Note: This is a best-effort scan of the buffer's current state.
        // Due to the lock-free nature, some entries may be in flight.
        // For accurate results, use flush() before querying.
        const head = buffer.head.load(.acquire);
        const tail = buffer.tail.load(.acquire);

        if (head == tail) return;

        var pos = tail;
        while (pos != head) : (pos +%= 1) {
            const slot = &buffer.buffer[pos & buffer.mask];
            const seq = slot.sequence.load(.acquire);

            // Check if slot has valid data (sequence > pos means data is written)
            const seq_i: isize = @intCast(seq);
            const expected: isize = @intCast(pos +% 1);
            if (seq_i >= expected) {
                const data = slot.data;
                if (data.series_id == series_id and data.timestamp >= start_ts and data.timestamp <= end_ts) {
                    try result.append(self.allocator, data);
                }
            }
        }
    }

    // ============ Internal: Tiered Storage ============

    fn flushChunkToWarm(self: *TSDB, chunk: *Chunk) !void {
        if (chunk.count == 0) return;
        if (self.tiered_storage == null) return;

        const ts = &self.tiered_storage.?;
        const data_dir = self.data_dir orelse return;

        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "warm/chunk_{d}_{d}.mmap", .{
            chunk.series_id,
            std.time.timestamp(),
        });

        const mmap = try MmapChunk.create(
            self.allocator,
            data_dir,
            path,
            chunk.series_id,
            chunk.timestamps[0..chunk.count],
            chunk.values[0..chunk.count],
            chunk.min_ts,
            chunk.max_ts,
        );

        try ts.warm_chunks.append(self.allocator, .{
            .series_id = chunk.series_id,
            .chunk = mmap,
            .created_ts = std.time.timestamp(),
        });

        ts.stats.flushes_to_warm += 1;
        ts.stats.warm_chunks += 1;
        ts.stats.warm_points += chunk.count;

        if (ts.warm_chunks.items.len > ts.config.max_warm_chunks) {
            ts.compactToCold() catch |err| {
                std.log.warn("cold compaction failed: {}", .{err});
            };
        }
    }
};

// ============ Tests ============

test "TSDB single-threaded insert and query" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_tsdb_st.bin",
        .concurrent_mode = false,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_tsdb_st.bin") catch {};

    // Insert data points
    try testing.expect(tsdb.insert(1, 1000, 10.5));
    try testing.expect(tsdb.insert(1, 2000, 20.5));
    try testing.expect(tsdb.insert(1, 3000, 30.5));
    try testing.expect(tsdb.insert(2, 1500, 15.5));

    // Query series 1
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try tsdb.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 3), result.items.len);

    // Query with time range
    result.clearRetainingCapacity();
    try tsdb.query(1, 1500, 2500, &result);
    try testing.expectEqual(@as(usize, 1), result.items.len);
    try testing.expectEqual(@as(i64, 2000), result.items[0].timestamp);
}

test "TSDB concurrent mode" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_tsdb_conc.bin",
        .concurrent_mode = true,
        .ingestion_buffer_capacity = 1024,
        .num_partitions = 2,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_tsdb_conc.bin") catch {};

    try tsdb.start();

    // Insert data
    try testing.expect(tsdb.insert(1, 1000, 10.0));
    try testing.expect(tsdb.insert(1, 2000, 20.0));
    try testing.expect(tsdb.insert(1, 3000, 30.0));

    // Wait for consumer
    Thread.sleep(50 * std.time.ns_per_ms);

    // Flush and query
    try tsdb.flush();

    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try tsdb.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 3), result.items.len);
}

test "TSDB concurrent producers" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_tsdb_producers.bin",
        .concurrent_mode = true,
        .ingestion_buffer_capacity = 8192,
        .num_partitions = 4,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_tsdb_producers.bin") catch {};

    try tsdb.start();

    const num_threads = 4;
    const items_per_thread = 250;

    var threads: [num_threads]Thread = undefined;
    for (0..num_threads) |t| {
        threads[t] = try Thread.spawn(.{}, struct {
            fn produce(db: *TSDB, thread_id: usize) void {
                for (0..items_per_thread) |i| {
                    const series_id: u64 = @intCast(thread_id + 1);
                    const timestamp: i64 = @intCast(i * 1000);
                    const value: f64 = @floatFromInt(thread_id * 1000 + i);

                    while (!db.insert(series_id, timestamp, value)) {
                        std.atomic.spinLoopHint();
                    }
                }
            }
        }.produce, .{ &tsdb, t });
    }

    for (&threads) |*t| {
        t.join();
    }

    Thread.sleep(100 * std.time.ns_per_ms);
    try tsdb.flush();

    var total_points: usize = 0;
    for (1..num_threads + 1) |series_id| {
        var result = std.ArrayList(DataPoint){};
        defer result.deinit(allocator);
        try tsdb.query(@intCast(series_id), 0, std.math.maxInt(i64), &result);
        total_points += result.items.len;
    }

    try testing.expectEqual(num_threads * items_per_thread, total_points);
}

test "TSDB getStats" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_tsdb_stats.bin",
        .concurrent_mode = false,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_tsdb_stats.bin") catch {};

    _ = tsdb.insert(1, 1000, 10.0);
    _ = tsdb.insert(1, 2000, 20.0);
    _ = tsdb.insert(2, 1000, 100.0);

    const stats = tsdb.getStats();
    try testing.expectEqual(@as(u32, 2), stats.series_count);
    try testing.expectEqual(@as(u64, 3), stats.total_points);
}

test "TSDB query latest" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_tsdb_latest.bin",
        .concurrent_mode = false,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_tsdb_latest.bin") catch {};

    _ = tsdb.insert(1, 1000, 10.0);
    _ = tsdb.insert(1, 2000, 20.0);
    _ = tsdb.insert(1, 3000, 30.0);

    const latest = tsdb.queryLatest(1);
    try testing.expect(latest != null);
    try testing.expectEqual(@as(i64, 3000), latest.?.timestamp);
    try testing.expectEqual(@as(f64, 30.0), latest.?.value);
}
