const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const Chunk = @import("chunk.zig").Chunk;
const WAL = @import("wal.zig").WAL;

pub const DataPoint = struct {
    series_id: u64,
    timestamp: i64,
    value: f64,
};

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
    chunk_capacity: usize,
    wal_serialize_buffer: std.ArrayList(u8),
    batch_size: usize,

    // Write buffer for batching inserts
    write_buffer: std.ArrayList(DataPoint),
    write_buffer_capacity: usize,

    const DEFAULT_CHUNK_CAPACITY: usize = 8192;
    const DEFAULT_WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB
    const DEFAULT_BATCH_SIZE: usize = 1000;
    const DEFAULT_WRITE_BUFFER_CAPACITY: usize = 1000;

    pub const Config = struct {
        chunk_capacity: usize = DEFAULT_CHUNK_CAPACITY,
        wal_segment_size: u64 = DEFAULT_WAL_SEGMENT_SIZE,
        batch_size: usize = DEFAULT_BATCH_SIZE,
        write_buffer_capacity: usize = DEFAULT_WRITE_BUFFER_CAPACITY,
        wal_path: []const u8,
    };

    pub fn init(allocator: Allocator, config: Config) !TSDB {
        const cwd = fs.cwd();
        const wal = try WAL.init(allocator, config.wal_segment_size, cwd, config.wal_path);

        // Pre-allocate write buffer
        var write_buffer = std.ArrayList(DataPoint){};
        try write_buffer.ensureTotalCapacity(allocator, config.write_buffer_capacity);

        return TSDB{
            .allocator = allocator,
            .wal = wal,
            .active_chunks = std.AutoHashMap(u64, *Chunk).init(allocator),
            .chunk_capacity = config.chunk_capacity,
            .wal_serialize_buffer = std.ArrayList(u8){},
            .batch_size = config.batch_size,
            .write_buffer = write_buffer,
            .write_buffer_capacity = config.write_buffer_capacity,
        };
    }

    pub fn deinit(self: *TSDB) void {
        // Flush any remaining buffered writes
        self.flushWriteBuffer() catch |err| {
            std.log.err("failed to flush write buffer on deinit: {}", .{err});
        };

        var it = self.active_chunks.valueIterator();
        while (it.next()) |chunk_ptr| {
            chunk_ptr.*.deinit(self.allocator);
            self.allocator.destroy(chunk_ptr.*);
        }
        self.active_chunks.deinit();
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

    // Insert multiple data points in batch for maximum performance
    pub fn insertBatch(self: *TSDB, entries: []const DataPoint) !void {
        if (entries.len == 0) return;

        // Write all entries to WAL in one batch
        try self.writeBatchToWAL(entries);

        // Insert into chunks
        for (entries) |entry| {
            const chunk = try self.getOrCreateChunk(entry.series_id);
            try chunk.insert(entry.timestamp, entry.value);
        }
    }

    // Query data points for a series within a time range
    pub fn query(self: *TSDB, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        // First check chunk data
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

        // Also check write buffer for unflushed data
        for (self.write_buffer.items) |entry| {
            if (entry.series_id == series_id and entry.timestamp >= start_ts and entry.timestamp <= end_ts) {
                try result.append(self.allocator, entry);
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

    // Flush write buffer to WAL and chunks
    pub fn flushWriteBuffer(self: *TSDB) !void {
        if (self.write_buffer.items.len == 0) return;

        // Write all buffered entries to WAL in single batch (queues to io_uring)
        try self.writeBatchToWAL(self.write_buffer.items);

        // Submit queued writes to kernel (non-blocking)
        _ = try self.wal.submit();

        // Insert all entries to chunks
        for (self.write_buffer.items) |entry| {
            const chunk = try self.getOrCreateChunk(entry.series_id);
            try chunk.insert(entry.timestamp, entry.value);
        }

        // Clear buffer but keep capacity
        self.write_buffer.clearRetainingCapacity();
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
            const chunk_ptr = try self.allocator.create(Chunk);
            chunk_ptr.* = try Chunk.init(self.allocator, series_id, self.chunk_capacity);
            entry.value_ptr.* = chunk_ptr;
        }

        return entry.value_ptr.*;
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
