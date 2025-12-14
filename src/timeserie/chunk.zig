const std = @import("std");
const testing = std.testing;
const mem = std.mem;
const Allocator = std.mem.Allocator;

pub const Chunk = struct {
    series_id: u64,
    timestamps: []i64,
    values: []f64,
    min_ts: i64,
    max_ts: i64,
    count: usize,
    capacity: usize,

    pub fn init(allocator: Allocator, series_id: u64, capacity: usize) !Chunk {
        const align64 = comptime mem.Alignment.fromByteUnits(64);

        return Chunk{
            .series_id = series_id,
            .timestamps = try allocator.alignedAlloc(i64, align64, capacity),
            .values = try allocator.alignedAlloc(f64, align64, capacity),
            .min_ts = std.math.maxInt(i64),
            .max_ts = std.math.minInt(i64),
            .count = 0,
            .capacity = capacity,
        };
    }

    pub fn deinit(self: *Chunk, allocator: Allocator) void {
        const align64 = comptime mem.Alignment.fromByteUnits(64);
        const timestamps_bytes: []align(64) u8 = @alignCast(mem.sliceAsBytes(self.timestamps));
        const values_bytes: []align(64) u8 = @alignCast(mem.sliceAsBytes(self.values));
        allocator.rawFree(timestamps_bytes, align64, @returnAddress());
        allocator.rawFree(values_bytes, align64, @returnAddress());
    }

    pub fn insert(self: *Chunk, timestamp: i64, value: f64) !void {
        if (self.count >= self.capacity) {
            return error.ChunkFull;
        }

        self.timestamps[self.count] = timestamp;
        self.values[self.count] = value;

        if (timestamp < self.min_ts) self.min_ts = timestamp;
        if (timestamp > self.max_ts) self.max_ts = timestamp;

        self.count += 1;
    }
};

test "Chunk insert" {
    const allocator = testing.allocator;

    var chunk = try Chunk.init(allocator, 42, 1024);
    defer chunk.deinit(allocator);

    try testing.expectEqual(@as(u64, 42), chunk.series_id);
    try testing.expectEqual(@as(usize, 0), chunk.count);
    try testing.expectEqual(@as(usize, 1024), chunk.capacity);

    try chunk.insert(1000, 3.14);
    try testing.expectEqual(@as(usize, 1), chunk.count);
    try testing.expectEqual(@as(i64, 1000), chunk.timestamps[0]);
    try testing.expectEqual(@as(f64, 3.14), chunk.values[0]);
    try testing.expectEqual(@as(i64, 1000), chunk.min_ts);
    try testing.expectEqual(@as(i64, 1000), chunk.max_ts);

    try chunk.insert(2000, 2.71);
    try testing.expectEqual(@as(usize, 2), chunk.count);
    try testing.expectEqual(@as(i64, 2000), chunk.timestamps[1]);
    try testing.expectEqual(@as(f64, 2.71), chunk.values[1]);
    try testing.expectEqual(@as(i64, 1000), chunk.min_ts);
    try testing.expectEqual(@as(i64, 2000), chunk.max_ts);
}
