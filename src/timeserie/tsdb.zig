const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const Chunk = @import("chunk.zig").Chunk;
const WAL = @import("wal.zig").WAL;

pub const TSDB = struct {
    allocator: Allocator,
    wal: WAL,
    active_chunks: std.AutoHashMap(u64, *Chunk),
    chunk_capacity: usize,
    wal_buffer: std.ArrayList(u8),
    batch_size: usize,

    const DEFAULT_CHUNK_CAPACITY: usize = 8192;
    const DEFAULT_WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB
    const DEFAULT_BATCH_SIZE: usize = 1000;

    pub const Config = struct {
        chunk_capacity: usize = DEFAULT_CHUNK_CAPACITY,
        wal_segment_size: u64 = DEFAULT_WAL_SEGMENT_SIZE,
        batch_size: usize = DEFAULT_BATCH_SIZE,
        wal_path: []const u8,
    };

    pub fn init(allocator: Allocator, config: Config) !TSDB {
        const cwd = fs.cwd();
        const wal = try WAL.init(allocator, config.wal_segment_size, cwd, config.wal_path);

        return TSDB{
            .allocator = allocator,
            .wal = wal,
            .active_chunks = std.AutoHashMap(u64, *Chunk).init(allocator),
            .chunk_capacity = config.chunk_capacity,
            .wal_buffer = std.ArrayList(u8){},
            .batch_size = config.batch_size,
        };
    }

    pub fn deinit(self: *TSDB) void {
        var it = self.active_chunks.valueIterator();
        while (it.next()) |chunk_ptr| {
            chunk_ptr.*.deinit(self.allocator);
            self.allocator.destroy(chunk_ptr.*);
        }
        self.active_chunks.deinit();
        self.wal_buffer.deinit(self.allocator);
        self.wal.deinit();
    }

    // Insert a single data point
    pub fn insert(self: *TSDB, series_id: u64, timestamp: i64, value: f64) !void {
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

    /// Query data points for a series within a time range
    pub fn query(self: *TSDB, series_id: u64, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        const chunk = self.active_chunks.get(series_id) orelse return;

        // Early exit if time range doesn't overlap with chunk
        if (end_ts < chunk.min_ts or start_ts > chunk.max_ts) {
            return;
        }

        // Scan chunk for matching timestamps
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

    // Query the latest value for a series
    pub fn queryLatest(self: *TSDB, series_id: u64) ?DataPoint {
        const chunk = self.active_chunks.get(series_id) orelse return null;
        if (chunk.count == 0) return null;

        const idx = chunk.count - 1;
        return DataPoint{
            .series_id = series_id,
            .timestamp = chunk.timestamps[idx],
            .value = chunk.values[idx],
        };
    }

    // Sync WAL to disk for durability
    pub fn sync(self: *TSDB) !void {
        try self.wal.sync();
    }

    // Flush pending WAL writes
    pub fn flush(self: *TSDB) !void {
        try self.wal.flush();
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
        self.wal_buffer.clearRetainingCapacity();

        const writer = self.wal_buffer.writer(self.allocator);
        try writer.writeInt(u64, series_id, .little);
        try writer.writeInt(i64, timestamp, .little);
        try writer.writeInt(u64, @bitCast(value), .little);

        _ = try self.wal.write(self.wal_buffer.items);
    }

    fn writeBatchToWAL(self: *TSDB, entries: []const DataPoint) !void {
        // Pre-allocate buffer for all entries
        const entry_size = @sizeOf(u64) + @sizeOf(i64) + @sizeOf(f64);
        try self.wal_buffer.ensureTotalCapacity(self.allocator, entry_size * entries.len);
        self.wal_buffer.clearRetainingCapacity();

        const writer = self.wal_buffer.writer(self.allocator);

        // Write all entries to buffer
        for (entries) |entry| {
            try writer.writeInt(u64, entry.series_id, .little);
            try writer.writeInt(i64, entry.timestamp, .little);
            try writer.writeInt(u64, @bitCast(entry.value), .little);
        }

        // Single WAL write for entire batch
        _ = try self.wal.write(self.wal_buffer.items);
    }
};

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

test "TSDB insert and query" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal.bin",
    });
    defer tsdb.deinit();
    // defer fs.cwd().deleteFile("/tmp/test_wal.bin") catch {};

    // Insert data points
    try tsdb.insert(1, 1000, 10.5);
    try tsdb.insert(1, 2000, 20.5);
    try tsdb.insert(1, 3000, 30.5);
    try tsdb.insert(2, 1500, 15.5);

    // Query series 1
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try tsdb.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 3), result.items.len);
    try testing.expectEqual(@as(i64, 1000), result.items[0].timestamp);
    try testing.expectEqual(@as(f64, 10.5), result.items[0].value);

    // Query with time range
    result.clearRetainingCapacity();
    try tsdb.query(1, 1500, 2500, &result);
    try testing.expectEqual(@as(usize, 1), result.items.len);
    try testing.expectEqual(@as(i64, 2000), result.items[0].timestamp);
}

test "TSDB batch insert" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal_batch.bin",
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_batch.bin") catch {};

    // Create batch of data points
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
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_latest.bin") catch {};

    try tsdb.insert(1, 1000, 10.0);
    try tsdb.insert(1, 2000, 20.0);
    try tsdb.insert(1, 3000, 30.0);

    const latest = tsdb.queryLatest(1);
    try testing.expect(latest != null);
    try testing.expectEqual(@as(i64, 3000), latest.?.timestamp);
    try testing.expectEqual(@as(f64, 30.0), latest.?.value);
}

test "TSDB statistics" {
    const allocator = testing.allocator;

    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_wal_stats.bin",
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_wal_stats.bin") catch {};

    try tsdb.insert(1, 1000, 10.0);
    try tsdb.insert(1, 2000, 20.0);
    try tsdb.insert(1, 3000, 30.0);

    const stats = tsdb.getStats(1);
    try testing.expect(stats != null);
    try testing.expectEqual(@as(usize, 3), stats.?.count);
    try testing.expectEqual(@as(f64, 10.0), stats.?.min_value);
    try testing.expectEqual(@as(f64, 30.0), stats.?.max_value);
    try testing.expectEqual(@as(f64, 20.0), stats.?.avg_value);
    try testing.expectEqual(@as(i64, 1000), stats.?.min_timestamp);
    try testing.expectEqual(@as(i64, 3000), stats.?.max_timestamp);
}
