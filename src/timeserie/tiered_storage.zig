const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const Allocator = std.mem.Allocator;

const chunk_mod = @import("chunk.zig");
const Chunk = chunk_mod.Chunk;
const ChunkPool = chunk_mod.ChunkPool;

const mmap_chunk_mod = @import("mmap_chunk.zig");
const MmapChunk = mmap_chunk_mod.MmapChunk;

const cold_storage_mod = @import("cold_storage.zig");
const ColdStorage = cold_storage_mod.ColdStorage;

pub const DataPoint = @import("../protocol/protocol.zig").DataPoint;

// Tiered storage manager coordinating hot, warm, and cold tiers.
//
// Hot tier: In-memory chunks for recent data and fast writes
// Warm tier: Memory-mapped chunks for completed data (zero-copy queries)
// Cold tier: Compressed archives for old data (load on demand)
//
// Data flow: Hot -> Warm -> Cold
pub const TieredStorage = struct {
    allocator: Allocator,
    dir: fs.Dir,
    config: Config,

    // Hot tier: active in-memory chunks
    chunk_pool: ChunkPool,
    hot_chunks: std.AutoHashMap(u64, *Chunk),

    // Warm tier: mmap'd completed chunks
    warm_chunks: std.ArrayList(WarmChunkEntry),

    // Cold tier: compressed archives
    cold_storage: ColdStorage,

    // Stats
    stats: Stats,

    pub const Config = struct {
        // Hot tier
        chunk_capacity: usize = 8192,
        chunk_pool_size: usize = 16,

        // Warm tier thresholds
        warm_dir: []const u8 = "warm",
        max_warm_chunks: usize = 100,
        warm_age_seconds: i64 = 3600, // 1 hour

        // Cold tier thresholds
        cold_dir: []const u8 = "cold",
        cold_age_seconds: i64 = 86400, // 24 hours
        cold_compression: ColdStorage.CompressionType = .deflate,
    };

    pub const Stats = struct {
        hot_chunks: usize = 0,
        hot_points: usize = 0,
        warm_chunks: usize = 0,
        warm_points: usize = 0,
        cold_archives: usize = 0,
        flushes_to_warm: u64 = 0,
        compactions_to_cold: u64 = 0,
    };

    const WarmChunkEntry = struct {
        series_id: u64,
        chunk: MmapChunk,
        created_ts: i64,
    };

    pub fn init(allocator: Allocator, base_dir: fs.Dir, config: Config) !TieredStorage {
        // Ensure warm and cold directories exist
        base_dir.makeDir(config.warm_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };
        base_dir.makeDir(config.cold_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        const cold_dir = try base_dir.openDir(config.cold_dir, .{});

        return TieredStorage{
            .allocator = allocator,
            .dir = base_dir,
            .config = config,
            .chunk_pool = try ChunkPool.init(allocator, config.chunk_capacity, config.chunk_pool_size),
            .hot_chunks = std.AutoHashMap(u64, *Chunk).init(allocator),
            .warm_chunks = std.ArrayList(WarmChunkEntry){},
            .cold_storage = ColdStorage.init(allocator, cold_dir),
            .stats = .{},
        };
    }

    pub fn deinit(self: *TieredStorage) void {
        // Release hot chunks
        var it = self.hot_chunks.valueIterator();
        while (it.next()) |chunk_ptr| {
            self.chunk_pool.release(chunk_ptr.*);
        }
        self.hot_chunks.deinit();

        // Close warm chunks
        for (self.warm_chunks.items) |*entry| {
            entry.chunk.deinit();
        }
        self.warm_chunks.deinit(self.allocator);

        // Close cold storage
        self.cold_storage.deinit();

        // Free pool
        self.chunk_pool.deinit();
    }

    // Insert a data point to hot tier
    pub fn insert(self: *TieredStorage, series_id: u64, timestamp: i64, value: f64) !void {
        const chunk = try self.getOrCreateHotChunk(series_id);

        // Check if chunk is full
        if (chunk.isFull()) {
            try self.flushToWarm(series_id, chunk);

            // Reset chunk for new data
            chunk.reset(series_id);
        }

        try chunk.insert(timestamp, value);
        self.stats.hot_points += 1;
    }

    // Insert multiple data points to hot tier
    pub fn insertBatch(self: *TieredStorage, entries: []const DataPoint) !void {
        for (entries) |entry| {
            try self.insert(entry.series_id, entry.timestamp, entry.value);
        }
    }

    // Query across all tiers
    pub fn query(
        self: *TieredStorage,
        series_id: u64,
        start_ts: i64,
        end_ts: i64,
        result: *std.ArrayList(DataPoint),
    ) !void {
        // Query hot tier
        if (self.hot_chunks.get(series_id)) |chunk| {
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

        // Query warm tier
        for (self.warm_chunks.items) |*entry| {
            if (entry.series_id == series_id) {
                var warm_result = std.ArrayList(mmap_chunk_mod.DataPoint){};
                defer warm_result.deinit(self.allocator);

                try entry.chunk.query(self.allocator, start_ts, end_ts, &warm_result);

                for (warm_result.items) |p| {
                    try result.append(self.allocator, .{
                        .series_id = p.series_id,
                        .timestamp = p.timestamp,
                        .value = p.value,
                    });
                }
            }
        }

        // Query cold tier
        var cold_result = std.ArrayList(cold_storage_mod.DataPoint){};
        defer cold_result.deinit(self.allocator);

        try self.cold_storage.query(series_id, start_ts, end_ts, &cold_result);

        for (cold_result.items) |p| {
            try result.append(self.allocator, .{
                .series_id = p.series_id,
                .timestamp = p.timestamp,
                .value = p.value,
            });
        }
    }

    // Flush a hot chunk to warm tier
    fn flushToWarm(self: *TieredStorage, series_id: u64, chunk: *Chunk) !void {
        if (chunk.count == 0) return;

        // Generate filename
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/chunk_{d}_{d}.mmap", .{
            self.config.warm_dir,
            series_id,
            std.time.timestamp(),
        });

        // Create mmap'd chunk file
        const mmap = try MmapChunk.create(
            self.allocator,
            self.dir,
            path,
            series_id,
            chunk.timestamps[0..chunk.count],
            chunk.values[0..chunk.count],
            chunk.min_ts,
            chunk.max_ts,
        );

        try self.warm_chunks.append(self.allocator, .{
            .series_id = series_id,
            .chunk = mmap,
            .created_ts = std.time.timestamp(),
        });

        self.stats.flushes_to_warm += 1;
        self.stats.warm_chunks += 1;
        self.stats.warm_points += chunk.count;
        self.stats.hot_points -= chunk.count;
    }

    // Compact warm chunks to cold storage (by age OR count threshold)
    pub fn compactToCold(self: *TieredStorage) !void {
        const now = std.time.timestamp();
        const age_threshold = now - self.config.cold_age_seconds;
        const force_compact = self.warm_chunks.items.len > self.config.max_warm_chunks;

        // Find warm chunks to compact (old ones, or all if over limit)
        var chunks_to_compact = std.ArrayList(ColdStorage.ChunkInput){};
        defer chunks_to_compact.deinit(self.allocator);

        var indices_to_remove = std.ArrayList(usize){};
        defer indices_to_remove.deinit(self.allocator);

        for (self.warm_chunks.items, 0..) |entry, i| {
            const should_compact = force_compact or entry.created_ts < age_threshold;
            if (should_compact) {
                try chunks_to_compact.append(self.allocator, .{
                    .series_id = entry.series_id,
                    .min_ts = entry.chunk.min_ts,
                    .max_ts = entry.chunk.max_ts,
                    .timestamps = entry.chunk.timestamps,
                    .values = entry.chunk.values,
                });
                try indices_to_remove.append(self.allocator, i);
            }
        }

        if (chunks_to_compact.items.len == 0) return;

        // Create cold archive (path relative to cold_dir)
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "archive_{d}.zta", .{
            now,
        });

        try self.cold_storage.createArchive(path, chunks_to_compact.items, self.config.cold_compression);

        // Remove compacted warm chunks (in reverse order to preserve indices)
        var i = indices_to_remove.items.len;
        while (i > 0) {
            i -= 1;
            const idx = indices_to_remove.items[i];
            var entry = self.warm_chunks.orderedRemove(idx);
            self.stats.warm_points -= entry.chunk.count;
            entry.chunk.deinit();
        }

        self.stats.warm_chunks -= indices_to_remove.items.len;
        self.stats.cold_archives += 1;
        self.stats.compactions_to_cold += 1;
    }

    // Fast bulk archive: directly to cold tier, skipping warm (for archival workloads)
    pub fn archiveBulk(self: *TieredStorage, entries: []const DataPoint) !void {
        if (entries.len == 0) return;

        // Group by series and build chunks
        var series_data = std.AutoHashMap(u64, struct {
            timestamps: std.ArrayList(i64),
            values: std.ArrayList(f64),
            min_ts: i64,
            max_ts: i64,
        }).init(self.allocator);
        defer {
            var it = series_data.valueIterator();
            while (it.next()) |v| {
                v.timestamps.deinit(self.allocator);
                v.values.deinit(self.allocator);
            }
            series_data.deinit();
        }

        for (entries) |e| {
            const gop = try series_data.getOrPut(e.series_id);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{
                    .timestamps = std.ArrayList(i64){},
                    .values = std.ArrayList(f64){},
                    .min_ts = std.math.maxInt(i64),
                    .max_ts = std.math.minInt(i64),
                };
            }
            try gop.value_ptr.timestamps.append(self.allocator, e.timestamp);
            try gop.value_ptr.values.append(self.allocator, e.value);
            if (e.timestamp < gop.value_ptr.min_ts) gop.value_ptr.min_ts = e.timestamp;
            if (e.timestamp > gop.value_ptr.max_ts) gop.value_ptr.max_ts = e.timestamp;
        }

        // Build chunk inputs for cold storage
        var chunk_inputs = std.ArrayList(ColdStorage.ChunkInput){};
        defer chunk_inputs.deinit(self.allocator);

        var it = series_data.iterator();
        while (it.next()) |kv| {
            try chunk_inputs.append(self.allocator, .{
                .series_id = kv.key_ptr.*,
                .min_ts = kv.value_ptr.min_ts,
                .max_ts = kv.value_ptr.max_ts,
                .timestamps = kv.value_ptr.timestamps.items,
                .values = kv.value_ptr.values.items,
            });
        }

        // Create cold archive directly (path relative to cold_dir)
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "archive_{d}.zta", .{
            std.time.timestamp(),
        });

        try self.cold_storage.createArchive(path, chunk_inputs.items, self.config.cold_compression);
        self.stats.cold_archives += 1;
        self.stats.compactions_to_cold += 1;
    }

    // Run maintenance (flush full chunks, compact old data)
    pub fn runMaintenance(self: *TieredStorage) !void {
        // Flush full hot chunks
        var full_chunks = std.ArrayList(u64){};
        defer full_chunks.deinit(self.allocator);

        var it = self.hot_chunks.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.isFull()) {
                try full_chunks.append(self.allocator, entry.key_ptr.*);
            }
        }

        for (full_chunks.items) |series_id| {
            if (self.hot_chunks.get(series_id)) |chunk| {
                try self.flushToWarm(series_id, chunk);
                chunk.reset(series_id);
            }
        }

        // Compact old warm chunks
        try self.compactToCold();
    }

    // Get storage statistics
    pub fn getStats(self: *const TieredStorage) Stats {
        var stats = self.stats;
        stats.hot_chunks = self.hot_chunks.count();
        stats.warm_chunks = self.warm_chunks.items.len;
        return stats;
    }

    fn getOrCreateHotChunk(self: *TieredStorage, series_id: u64) !*Chunk {
        const entry = try self.hot_chunks.getOrPut(series_id);

        if (!entry.found_existing) {
            const chunk_ptr = try self.chunk_pool.acquire(series_id);
            entry.value_ptr.* = chunk_ptr;
            self.stats.hot_chunks += 1;
        }

        return entry.value_ptr.*;
    }
};

// Tests

test "TieredStorage hot tier insert and query" {
    const allocator = testing.allocator;

    // Create temp directory
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var storage = try TieredStorage.init(allocator, tmp_dir.dir, .{
        .chunk_capacity = 100,
    });
    defer storage.deinit();

    // Insert to hot tier
    try storage.insert(1, 1000, 10.0);
    try storage.insert(1, 2000, 20.0);
    try storage.insert(1, 3000, 30.0);

    // Query
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try storage.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 3), result.items.len);

    // Check stats
    const stats = storage.getStats();
    try testing.expectEqual(@as(usize, 1), stats.hot_chunks);
    try testing.expectEqual(@as(usize, 3), stats.hot_points);
}

test "TieredStorage flush to warm" {
    const allocator = testing.allocator;

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var storage = try TieredStorage.init(allocator, tmp_dir.dir, .{
        .chunk_capacity = 5, // Small chunks for testing
    });
    defer storage.deinit();

    // Insert enough to trigger flush
    for (0..10) |i| {
        try storage.insert(1, @intCast(i * 1000), @floatFromInt(i));
    }

    // Should have flushed at least once
    const stats = storage.getStats();
    try testing.expect(stats.flushes_to_warm >= 1);
    try testing.expect(stats.warm_chunks >= 1);

    // Query should find all data
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try storage.query(1, 0, 20000, &result);
    try testing.expectEqual(@as(usize, 10), result.items.len);
}

test "TieredStorage multiple series" {
    const allocator = testing.allocator;

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var storage = try TieredStorage.init(allocator, tmp_dir.dir, .{});
    defer storage.deinit();

    // Insert to multiple series
    try storage.insert(1, 1000, 1.0);
    try storage.insert(2, 1000, 2.0);
    try storage.insert(3, 1000, 3.0);
    try storage.insert(1, 2000, 1.1);
    try storage.insert(2, 2000, 2.1);

    // Query each series
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try storage.query(1, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 2), result.items.len);

    result.clearRetainingCapacity();
    try storage.query(2, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 2), result.items.len);

    result.clearRetainingCapacity();
    try storage.query(3, 0, 5000, &result);
    try testing.expectEqual(@as(usize, 1), result.items.len);
}
