const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const testing = std.testing;
const Allocator = std.mem.Allocator;

const Chunk = @import("chunk.zig").Chunk;
const ChunkPool = @import("chunk.zig").ChunkPool;
const WAL = @import("wal.zig").WAL;
const tiered_mod = @import("tiered_storage.zig");
const TieredStorage = tiered_mod.TieredStorage;
const ColdStorage = @import("cold_storage.zig").ColdStorage;
const MmapChunk = @import("mmap_chunk.zig").MmapChunk;

pub const DataPoint = @import("../protocol/protocol.zig").DataPoint;

pub const SeriesStats = struct {
    count: usize,
    min_value: f64,
    max_value: f64,
    avg_value: f64,
    min_timestamp: i64,
    max_timestamp: i64,
};

pub const TSDB = struct {
    allocator: Allocator,
    wal: WAL,
    active_chunks: std.AutoHashMap(u64, *Chunk),
    chunk_pool: ChunkPool,
    chunk_capacity: usize,
    wal_serialize_buffer: std.ArrayList(u8),
    batch_size: usize,

    // Write buffer for batching inserts
    write_buffer: std.ArrayList(DataPoint),
    write_buffer_capacity: usize,

    // Optional tiered storage for warm/cold data
    tiered_storage: ?TieredStorage,
    data_dir: ?fs.Dir,

    pub const DEFAULT_CHUNK_CAPACITY: usize = 8192;
    pub const DEFAULT_WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB
    pub const DEFAULT_BATCH_SIZE: usize = 1000;
    pub const DEFAULT_WRITE_BUFFER_CAPACITY: usize = 1000;
    pub const DEFAULT_CHUNK_POOL_SIZE: usize = 16;

    pub const DEFAULT_MAX_WARM_CHUNKS: usize = 100;

    pub const Config = struct {
        chunk_capacity: usize = DEFAULT_CHUNK_CAPACITY,
        wal_segment_size: u64 = DEFAULT_WAL_SEGMENT_SIZE,
        batch_size: usize = DEFAULT_BATCH_SIZE,
        write_buffer_capacity: usize = DEFAULT_WRITE_BUFFER_CAPACITY,
        chunk_pool_size: usize = DEFAULT_CHUNK_POOL_SIZE,
        wal_path: []const u8,
        // Tiered storage config (optional)
        enable_tiered_storage: bool = false,
        data_dir: ?[]const u8 = null, // Directory for warm/cold storage
        max_warm_chunks: usize = DEFAULT_MAX_WARM_CHUNKS, // Threshold to trigger cold compaction
    };

    pub fn init(allocator: Allocator, config: Config) !TSDB {
        const cwd = fs.cwd();
        const wal = try WAL.init(allocator, config.wal_segment_size, cwd, config.wal_path);

        // Pre-allocate write buffer
        var write_buffer = std.ArrayList(DataPoint){};
        try write_buffer.ensureTotalCapacity(allocator, config.write_buffer_capacity);

        // Pre-allocate chunk pool
        const chunk_pool = try ChunkPool.init(allocator, config.chunk_capacity, config.chunk_pool_size);

        // Optional tiered storage
        var tiered_storage: ?TieredStorage = null;
        var data_dir: ?fs.Dir = null;

        if (config.enable_tiered_storage) {
            if (config.data_dir) |dir_path| {
                // Create data directory if it doesn't exist
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

        return TSDB{
            .allocator = allocator,
            .wal = wal,
            .active_chunks = std.AutoHashMap(u64, *Chunk).init(allocator),
            .chunk_pool = chunk_pool,
            .chunk_capacity = config.chunk_capacity,
            .wal_serialize_buffer = std.ArrayList(u8){},
            .batch_size = config.batch_size,
            .write_buffer = write_buffer,
            .write_buffer_capacity = config.write_buffer_capacity,
            .tiered_storage = tiered_storage,
            .data_dir = data_dir,
        };
    }

    pub fn deinit(self: *TSDB) void {
        // Flush any remaining buffered writes
        self.flushWriteBuffer() catch |err| {
            std.log.err("failed to flush write buffer on deinit: {}", .{err});
        };

        // Release all active chunks back to pool
        var it = self.active_chunks.valueIterator();
        while (it.next()) |chunk_ptr| {
            self.chunk_pool.release(chunk_ptr.*);
        }
        self.active_chunks.deinit();

        // Deinit pool (frees all pooled chunks)
        self.chunk_pool.deinit();

        // Deinit tiered storage if enabled
        if (self.tiered_storage) |*ts| {
            ts.deinit();
        }
        if (self.data_dir) |dir| {
            var d = dir;
            d.close();
        }

        self.wal_serialize_buffer.deinit(self.allocator);
        self.write_buffer.deinit(self.allocator);
        self.wal.deinit();
    }

    // Insert a single data point (buffered for performance)
    pub fn insert(self: *TSDB, series_id: u64, timestamp: i64, value: f64) !void {
        // Fast path: append to write buffer
        try self.write_buffer.append(self.allocator, .{
            .series_id = series_id,
            .timestamp = timestamp,
            .value = value,
        });

        // Flush when buffer is full
        if (self.write_buffer.items.len >= self.write_buffer_capacity) {
            try self.flushWriteBuffer();
        }
    }

    // Insert bypassing buffer when durability is critical
    pub fn insertImmediate(self: *TSDB, series_id: u64, timestamp: i64, value: f64) !void {
        // Write to WAL first for durability
        try self.writeToWAL(series_id, timestamp, value);

        // Get or create chunk for this series
        const chunk = try self.getOrCreateChunk(series_id);

        // Insert into chunk
        try chunk.insert(timestamp, value);
    }

    // Insert multiple data points in batch (uses same buffer path as insert)
    pub fn insertBatch(self: *TSDB, entries: []const DataPoint) !void {
        if (entries.len == 0) return;

        for (entries) |entry| {
            try self.write_buffer.append(self.allocator, entry);

            // Flush when buffer is full
            if (self.write_buffer.items.len >= self.write_buffer_capacity) {
                try self.flushWriteBuffer();
            }
        }
    }

    // Query data points for a series within a time range
    pub fn query(self: *TSDB, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        // First check hot tier (active chunks)
        if (self.active_chunks.get(series_id)) |chunk| {
            // Only scan if time range overlaps with chunk
            if (!(end_ts < chunk.min_ts or start_ts > chunk.max_ts)) {
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
        }

        // Check write buffer for unflushed data
        for (self.write_buffer.items) |entry| {
            if (entry.series_id == series_id and entry.timestamp >= start_ts and entry.timestamp <= end_ts) {
                try result.append(self.allocator, entry);
            }
        }

        // Check warm/cold tiers if enabled
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

    // Query the latest value for a series
    pub fn queryLatest(self: *TSDB, series_id: u64) ?DataPoint {
        var latest: ?DataPoint = null;

        // Check chunk data
        if (self.active_chunks.get(series_id)) |chunk| {
            if (chunk.count > 0) {
                const idx = chunk.count - 1;
                latest = DataPoint{
                    .series_id = series_id,
                    .timestamp = chunk.timestamps[idx],
                    .value = chunk.values[idx],
                };
            }
        }

        // Check write buffer for newer data
        for (self.write_buffer.items) |entry| {
            if (entry.series_id == series_id) {
                if (latest == null or entry.timestamp > latest.?.timestamp) {
                    latest = entry;
                }
            }
        }

        return latest;
    }

    // Sync WAL to disk for durability
    pub fn sync(self: *TSDB) !void {
        try self.flushWriteBuffer();
        try self.wal.sync();
    }

    // Flush pending WAL writes
    pub fn flush(self: *TSDB) !void {
        try self.flushWriteBuffer();
        try self.wal.flush();
    }

    // Run maintenance tasks (compact warm to cold, etc.)
    pub fn runMaintenance(self: *TSDB) !void {
        if (self.tiered_storage) |*ts| {
            try ts.runMaintenance();
        }
    }

    // Check if tiered storage is enabled
    pub fn hasTieredStorage(self: *const TSDB) bool {
        return self.tiered_storage != null;
    }

    // Get tiered storage stats
    pub fn getTieredStats(self: *const TSDB) ?TieredStorage.Stats {
        if (self.tiered_storage) |ts| {
            return ts.getStats();
        }
        return null;
    }

    // Flush write buffer to chunks (memory-only for speed)
    // WAL is written when chunks flush to warm tier for durability
    pub fn flushWriteBuffer(self: *TSDB) !void {
        if (self.write_buffer.items.len == 0) return;

        // Group entries by series_id for batch insert
        // For now, simple approach: process in order but use batch insert per chunk
        var current_series: u64 = 0;
        var batch_start: usize = 0;

        for (self.write_buffer.items, 0..) |entry, i| {
            if (i == 0) {
                current_series = entry.series_id;
                continue;
            }

            // When series changes or at end, flush the batch
            if (entry.series_id != current_series) {
                try self.flushBatchToChunk(current_series, self.write_buffer.items[batch_start..i]);
                current_series = entry.series_id;
                batch_start = i;
            }
        }

        // Flush remaining batch
        if (batch_start < self.write_buffer.items.len) {
            try self.flushBatchToChunk(current_series, self.write_buffer.items[batch_start..]);
        }

        // Clear buffer but keep capacity
        self.write_buffer.clearRetainingCapacity();
    }

    // Flush a batch of same-series entries to chunk using vectorized insert
    fn flushBatchToChunk(self: *TSDB, series_id: u64, entries: []const DataPoint) !void {
        if (entries.len == 0) return;

        // Extract timestamps and values for batch insert
        // Use stack buffer for small batches, allocate for large
        var ts_buf: [1024]i64 = undefined;
        var val_buf: [1024]f64 = undefined;

        var timestamps: []i64 = undefined;
        var values: []f64 = undefined;
        var allocated = false;

        if (entries.len <= 1024) {
            timestamps = ts_buf[0..entries.len];
            values = val_buf[0..entries.len];
        } else {
            timestamps = try self.allocator.alloc(i64, entries.len);
            values = try self.allocator.alloc(f64, entries.len);
            allocated = true;
        }
        defer if (allocated) {
            self.allocator.free(timestamps);
            self.allocator.free(values);
        };

        for (entries, 0..) |e, i| {
            timestamps[i] = e.timestamp;
            values[i] = e.value;
        }

        // Insert in batches, handling chunk boundaries
        var offset: usize = 0;
        while (offset < entries.len) {
            const chunk = try self.getOrCreateChunk(series_id);
            const inserted = chunk.insertBatch(timestamps[offset..], values[offset..]);
            offset += inserted;

            // If nothing inserted, chunk is full - will be flushed on next getOrCreateChunk
            if (inserted == 0) {
                try self.flushChunkToWarm(chunk);
                chunk.reset(series_id);
            }
        }
    }

    // Get statistics for a series
    pub fn getStats(self: *TSDB, series_id: u64) ?SeriesStats {
        const chunk = self.active_chunks.get(series_id) orelse return null;
        if (chunk.count == 0) return null;

        var min_val: f64 = std.math.inf(f64);
        var max_val: f64 = -std.math.inf(f64);
        var sum: f64 = 0;

        for (chunk.values[0..chunk.count]) |val| {
            if (val < min_val) min_val = val;
            if (val > max_val) max_val = val;
            sum += val;
        }

        return SeriesStats{
            .count = chunk.count,
            .min_value = min_val,
            .max_value = max_val,
            .avg_value = sum / @as(f64, @floatFromInt(chunk.count)),
            .min_timestamp = chunk.min_ts,
            .max_timestamp = chunk.max_ts,
        };
    }

    // Internal functions

    fn getOrCreateChunk(self: *TSDB, series_id: u64) !*Chunk {
        const entry = try self.active_chunks.getOrPut(series_id);

        if (!entry.found_existing) {
            // Get chunk from pool (fast path - no allocation if pool has chunks)
            const chunk_ptr = try self.chunk_pool.acquire(series_id);
            entry.value_ptr.* = chunk_ptr;
        }

        const chunk = entry.value_ptr.*;

        // If chunk is full, flush to warm tier and reset
        if (chunk.isFull()) {
            try self.flushChunkToWarm(chunk);
            chunk.reset(series_id);
        }

        return chunk;
    }

    // Flush a full chunk to warm tier storage
    // Note: WAL not used here since mmap files provide durability via fsync
    fn flushChunkToWarm(self: *TSDB, chunk: *Chunk) !void {
        if (chunk.count == 0) return;

        // If no tiered storage, just reset chunk (pure in-memory mode)
        if (self.tiered_storage == null) return;

        const ts = &self.tiered_storage.?;
        const data_dir = self.data_dir orelse return;

        // Generate filename for warm chunk
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "warm/chunk_{d}_{d}.mmap", .{
            chunk.series_id,
            std.time.timestamp(),
        });

        // Create mmap'd chunk file
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

        // Auto-compact to cold when warm tier exceeds threshold
        if (ts.warm_chunks.items.len > ts.config.max_warm_chunks) {
            ts.compactToCold() catch |err| {
                std.log.warn("cold compaction failed: {}", .{err});
            };
        }
    }

    // Write entire chunk to WAL in batched operations (WAL queue has limited depth)
    fn writeChunkToWAL(self: *TSDB, chunk: *Chunk) !void {
        if (chunk.count == 0) return;

        const WAL_BATCH_SIZE: usize = 200; // Stay under io_uring queue depth (256)

        var offset: usize = 0;
        while (offset < chunk.count) {
            const batch_end = @min(offset + WAL_BATCH_SIZE, chunk.count);
            const batch_size = batch_end - offset;

            // Build batch of data points
            const batch = try self.allocator.alloc(DataPoint, batch_size);
            defer self.allocator.free(batch);

            for (0..batch_size) |i| {
                batch[i] = .{
                    .series_id = chunk.series_id,
                    .timestamp = chunk.timestamps[offset + i],
                    .value = chunk.values[offset + i],
                };
            }

            try self.writeBatchToWAL(batch);
            _ = try self.wal.submit();
            try self.wal.flush();

            offset = batch_end;
        }
    }

    fn writeToWAL(self: *TSDB, series_id: u64, timestamp: i64, value: f64) !void {
        self.wal_serialize_buffer.clearRetainingCapacity();

        const writer = self.wal_serialize_buffer.writer(self.allocator);
        try writer.writeInt(u64, series_id, .little);
        try writer.writeInt(i64, timestamp, .little);
        try writer.writeInt(u64, @bitCast(value), .little);

        _ = try self.wal.write(self.wal_serialize_buffer.items);
    }

    fn writeBatchToWAL(self: *TSDB, entries: []const DataPoint) !void {
        // Pre-allocate buffer for all entries
        const entry_size = @sizeOf(u64) + @sizeOf(i64) + @sizeOf(f64);
        try self.wal_serialize_buffer.ensureTotalCapacity(self.allocator, entry_size * entries.len);
        self.wal_serialize_buffer.clearRetainingCapacity();

        const writer = self.wal_serialize_buffer.writer(self.allocator);

        // Write all entries to buffer
        for (entries) |entry| {
            try writer.writeInt(u64, entry.series_id, .little);
            try writer.writeInt(i64, entry.timestamp, .little);
            try writer.writeInt(u64, @bitCast(entry.value), .little);
        }

        // Single WAL write for entire batch
        _ = try self.wal.write(self.wal_serialize_buffer.items);
    }
};

test "TSDB insert and query" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal.bin",
        .write_buffer_capacity = 10, // Small buffer for testing
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal.bin") catch {};

    // Insert data points
    try tsdb.insert(1, 1000, 10.5);
    try tsdb.insert(1, 2000, 20.5);
    try tsdb.insert(1, 3000, 30.5);
    try tsdb.insert(2, 1500, 15.5);

    // Query series 1 (should find data in write buffer)
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

test "TSDB write buffer flush" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal_flush.bin",
        .write_buffer_capacity = 3, // Flush after 3 inserts
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_flush.bin") catch {};

    // Insert 2 points (below threshold)
    try tsdb.insert(1, 1000, 10.0);
    try tsdb.insert(1, 2000, 20.0);
    try testing.expectEqual(@as(usize, 2), tsdb.write_buffer.items.len);

    // Insert 3rd point (triggers flush)
    try tsdb.insert(1, 3000, 30.0);
    try testing.expectEqual(@as(usize, 0), tsdb.write_buffer.items.len);

    // Data should now be in chunk
    const chunk = tsdb.active_chunks.get(1).?;
    try testing.expectEqual(@as(usize, 3), chunk.count);
}

test "TSDB batch insert" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal_batch.bin",
        .write_buffer_capacity = 10,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_batch.bin") catch {};

    // Create batch of data points (bypasses write buffer)
    const batch = [_]DataPoint{
        .{ .series_id = 1, .timestamp = 1000, .value = 10.0 },
        .{ .series_id = 1, .timestamp = 2000, .value = 20.0 },
        .{ .series_id = 1, .timestamp = 3000, .value = 30.0 },
        .{ .series_id = 2, .timestamp = 1000, .value = 100.0 },
    };

    try tsdb.insertBatch(&batch);

    // Verify insertions
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try tsdb.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 3), result.items.len);

    result.clearRetainingCapacity();
    try tsdb.query(2, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 1), result.items.len);
    try testing.expectEqual(@as(f64, 100.0), result.items[0].value);
}

test "TSDB query latest" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal_latest.bin",
        .write_buffer_capacity = 10,
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_latest.bin") catch {};

    try tsdb.insert(1, 1000, 10.0);
    try tsdb.insert(1, 2000, 20.0);
    try tsdb.insert(1, 3000, 30.0);

    // Should find latest in write buffer
    const latest = tsdb.queryLatest(1);
    try testing.expect(latest != null);
    try testing.expectEqual(@as(i64, 3000), latest.?.timestamp);
    try testing.expectEqual(@as(f64, 30.0), latest.?.value);
}

test "TSDB statistics" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal_stats.bin",
        .write_buffer_capacity = 2, // Force flush after 2 inserts
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_stats.bin") catch {};

    try tsdb.insert(1, 1000, 10.0);
    try tsdb.insert(1, 2000, 20.0);
    try tsdb.insert(1, 3000, 30.0); // Triggers flush
    try tsdb.flushWriteBuffer(); // Flush remaining

    const stats = tsdb.getStats(1);
    try testing.expect(stats != null);
    try testing.expectEqual(@as(usize, 3), stats.?.count);
    try testing.expectEqual(@as(f64, 10.0), stats.?.min_value);
    try testing.expectEqual(@as(f64, 30.0), stats.?.max_value);
    try testing.expectEqual(@as(f64, 20.0), stats.?.avg_value);
    try testing.expectEqual(@as(i64, 1000), stats.?.min_timestamp);
    try testing.expectEqual(@as(i64, 3000), stats.?.max_timestamp);
}

test "TSDB with tiered storage" {
    const allocator = testing.allocator;

    // Create temp directory for tiered storage
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Create WAL file path
    var wal_path_buf: [256]u8 = undefined;
    const wal_path = try tmp_dir.dir.realpath(".", &wal_path_buf);
    var full_wal_path: [512]u8 = undefined;
    const wal_file = try std.fmt.bufPrint(&full_wal_path, "{s}/test_tiered.wal", .{wal_path});

    // Create data dir path
    var data_path_buf: [256]u8 = undefined;
    const data_path = try tmp_dir.dir.realpath(".", &data_path_buf);

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = wal_file,
        .chunk_capacity = 10, // Small chunks to trigger warm flushes
        .write_buffer_capacity = 5,
        .enable_tiered_storage = true,
        .data_dir = data_path,
    });
    defer tsdb.deinit();

    // Verify tiered storage is enabled
    try testing.expect(tsdb.hasTieredStorage());

    // Insert enough data to fill a chunk and trigger warm flush
    for (0..20) |i| {
        try tsdb.insert(1, @intCast(i * 1000), @floatFromInt(i));
    }

    // Query should find all data (from hot and possibly warm tiers)
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try tsdb.query(1, 0, 30000, &result);
    try testing.expectEqual(@as(usize, 20), result.items.len);

    // Check tiered stats
    const tiered_stats = tsdb.getTieredStats();
    try testing.expect(tiered_stats != null);
}
