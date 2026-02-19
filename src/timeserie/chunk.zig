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

    /// Batch insert using vectorized memcpy - much faster for large batches
    /// Returns number of points actually inserted (may be less if chunk fills)
    pub fn insertBatch(self: *Chunk, timestamps: []const i64, values: []const f64) usize {
        const available = self.capacity - self.count;
        const to_insert = @min(timestamps.len, @min(values.len, available));

        if (to_insert == 0) return 0;

        // Vectorized copy
        @memcpy(self.timestamps[self.count..][0..to_insert], timestamps[0..to_insert]);
        @memcpy(self.values[self.count..][0..to_insert], values[0..to_insert]);

        // Update min/max timestamps
        for (timestamps[0..to_insert]) |ts| {
            if (ts < self.min_ts) self.min_ts = ts;
            if (ts > self.max_ts) self.max_ts = ts;
        }

        self.count += to_insert;
        return to_insert;
    }

    /// Returns remaining capacity
    pub fn remaining(self: *const Chunk) usize {
        return self.capacity - self.count;
    }

    // Reset chunk for reuse (keeps allocated memory)
    pub fn reset(self: *Chunk, new_series_id: u64) void {
        self.series_id = new_series_id;
        self.min_ts = std.math.maxInt(i64);
        self.max_ts = std.math.minInt(i64);
        self.count = 0;
        // Note: timestamps and values arrays keep their allocation
    }

    // Check if chunk is full
    pub fn isFull(self: *const Chunk) bool {
        return self.count >= self.capacity;
    }
};

// Pre-allocated pool of chunks to avoid allocation during hot path
pub const ChunkPool = struct {
    allocator: Allocator,
    free_chunks: std.ArrayList(*Chunk),
    chunk_capacity: usize,
    pool_size: usize,

    const DEFAULT_POOL_SIZE: usize = 16;

    pub fn init(allocator: Allocator, chunk_capacity: usize, pool_size: usize) !ChunkPool {
        var pool = ChunkPool{
            .allocator = allocator,
            .free_chunks = std.ArrayList(*Chunk){},
            .chunk_capacity = chunk_capacity,
            .pool_size = pool_size,
        };

        // Pre-allocate chunks
        try pool.free_chunks.ensureTotalCapacity(allocator, pool_size);
        for (0..pool_size) |_| {
            const chunk_ptr = try allocator.create(Chunk);
            chunk_ptr.* = try Chunk.init(allocator, 0, chunk_capacity);
            try pool.free_chunks.append(allocator, chunk_ptr);
        }

        return pool;
    }

    pub fn deinit(self: *ChunkPool) void {
        // Free all chunks in the pool
        for (self.free_chunks.items) |chunk_ptr| {
            chunk_ptr.deinit(self.allocator);
            self.allocator.destroy(chunk_ptr);
        }
        self.free_chunks.deinit(self.allocator);
    }

    // Get a chunk from pool (or allocate new if pool empty)
    pub fn acquire(self: *ChunkPool, series_id: u64) !*Chunk {
        if (self.free_chunks.pop()) |chunk_ptr| {
            // Reuse from pool
            chunk_ptr.reset(series_id);
            return chunk_ptr;
        }

        // Pool empty, allocate new chunk
        const chunk_ptr = try self.allocator.create(Chunk);
        chunk_ptr.* = try Chunk.init(self.allocator, series_id, self.chunk_capacity);
        return chunk_ptr;
    }

    // Return a chunk to the pool for reuse
    pub fn release(self: *ChunkPool, chunk_ptr: *Chunk) void {
        // Only return to pool if under capacity, otherwise free
        if (self.free_chunks.items.len < self.pool_size) {
            self.free_chunks.append(self.allocator, chunk_ptr) catch {
                // Failed to add to pool, just free it
                chunk_ptr.deinit(self.allocator);
                self.allocator.destroy(chunk_ptr);
            };
        } else {
            chunk_ptr.deinit(self.allocator);
            self.allocator.destroy(chunk_ptr);
        }
    }

    // Number of chunks currently available in pool
    pub fn available(self: *const ChunkPool) usize {
        return self.free_chunks.items.len;
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

test "Chunk reset" {
    const allocator = testing.allocator;

    var chunk = try Chunk.init(allocator, 1, 1024);
    defer chunk.deinit(allocator);

    // Insert some data
    try chunk.insert(1000, 10.0);
    try chunk.insert(2000, 20.0);
    try testing.expectEqual(@as(usize, 2), chunk.count);

    // Reset for new series
    chunk.reset(99);
    try testing.expectEqual(@as(u64, 99), chunk.series_id);
    try testing.expectEqual(@as(usize, 0), chunk.count);
    try testing.expectEqual(std.math.maxInt(i64), chunk.min_ts);
    try testing.expectEqual(std.math.minInt(i64), chunk.max_ts);

    // Can insert again
    try chunk.insert(3000, 30.0);
    try testing.expectEqual(@as(usize, 1), chunk.count);
    try testing.expectEqual(@as(i64, 3000), chunk.timestamps[0]);
}

test "ChunkPool acquire and release" {
    const allocator = testing.allocator;

    var pool = try ChunkPool.init(allocator, 1024, 4);
    defer pool.deinit();

    // Pool starts with 4 chunks
    try testing.expectEqual(@as(usize, 4), pool.available());

    // Acquire a chunk
    const chunk1 = try pool.acquire(1);
    try testing.expectEqual(@as(usize, 3), pool.available());
    try testing.expectEqual(@as(u64, 1), chunk1.series_id);

    // Acquire another
    const chunk2 = try pool.acquire(2);
    try testing.expectEqual(@as(usize, 2), pool.available());

    // Release first chunk back to pool
    pool.release(chunk1);
    try testing.expectEqual(@as(usize, 3), pool.available());

    // Acquire again - should get recycled chunk
    const chunk3 = try pool.acquire(3);
    try testing.expectEqual(@as(usize, 2), pool.available());
    try testing.expectEqual(@as(u64, 3), chunk3.series_id);
    try testing.expectEqual(@as(usize, 0), chunk3.count); // Should be reset

    // Cleanup
    pool.release(chunk2);
    pool.release(chunk3);
}

test "ChunkPool grows beyond initial size" {
    const allocator = testing.allocator;

    var pool = try ChunkPool.init(allocator, 1024, 2);
    defer pool.deinit();

    // Acquire more than pool size
    const chunk1 = try pool.acquire(1);
    const chunk2 = try pool.acquire(2);
    const chunk3 = try pool.acquire(3); // Allocated fresh
    const chunk4 = try pool.acquire(4); // Allocated fresh

    try testing.expectEqual(@as(usize, 0), pool.available());

    // Release all back
    pool.release(chunk1);
    pool.release(chunk2);
    // These two won't fit in pool (size=2), will be freed
    pool.release(chunk3);
    pool.release(chunk4);

    try testing.expectEqual(@as(usize, 2), pool.available());
}
