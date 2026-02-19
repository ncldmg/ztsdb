const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

pub const DataPoint = @import("../protocol/protocol.zig").DataPoint;

// Lock-free MPSC (Multiple Producer, Single Consumer) ring buffer.
// Uses sequence numbers per slot for coordination without locks.
// Based on Dmitry Vyukov's bounded MPSC queue algorithm.
pub const LockFreeBuffer = struct {
    const Slot = struct {
        data: DataPoint,
        sequence: std.atomic.Value(usize),
    };

    buffer: []Slot,
    capacity: usize,
    mask: usize, // capacity - 1 for fast modulo (requires power of 2)
    head: std.atomic.Value(usize), // next write position (producers)
    tail: std.atomic.Value(usize), // next read position (consumer)
    allocator: Allocator,

    // Stats for monitoring
    push_count: std.atomic.Value(u64),
    pop_count: std.atomic.Value(u64),
    failed_pushes: std.atomic.Value(u64),

    pub const Error = error{
        BufferFull,
    };

    // Initialize buffer with given capacity (rounded up to power of 2)
    pub fn init(allocator: Allocator, requested_capacity: usize) !LockFreeBuffer {
        // Capacity must be power of 2 for fast modulo via bitmask
        const capacity = try std.math.ceilPowerOfTwo(usize, requested_capacity);

        const buffer = try allocator.alloc(Slot, capacity);

        // Initialize sequence numbers: slot[i].sequence = i
        for (buffer, 0..) |*slot, i| {
            slot.* = .{
                .data = undefined,
                .sequence = std.atomic.Value(usize).init(i),
            };
        }

        return .{
            .buffer = buffer,
            .capacity = capacity,
            .mask = capacity - 1,
            .head = std.atomic.Value(usize).init(0),
            .tail = std.atomic.Value(usize).init(0),
            .allocator = allocator,
            .push_count = std.atomic.Value(u64).init(0),
            .pop_count = std.atomic.Value(u64).init(0),
            .failed_pushes = std.atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *LockFreeBuffer) void {
        self.allocator.free(self.buffer);
    }

    // Push a data point (thread-safe, can be called from multiple threads)
    // Returns false if buffer is full
    pub fn push(self: *LockFreeBuffer, point: DataPoint) bool {
        var pos = self.head.load(.monotonic);

        while (true) {
            const slot = &self.buffer[pos & self.mask];
            const seq = slot.sequence.load(.acquire);

            // Calculate difference between sequence and expected position
            const seq_i: isize = @intCast(seq);
            const pos_i: isize = @intCast(pos);
            const diff = seq_i - pos_i;

            if (diff == 0) {
                // Slot is available for writing, try to claim it
                if (self.head.cmpxchgWeak(pos, pos +% 1, .monotonic, .monotonic)) |old_pos| {
                    // Compare and swap failed, another producer claimed it, retry with new position
                    pos = old_pos;
                    continue;
                }
                // Successfully claimed slot, write data
                slot.data = point;
                // Mark slot as ready for consumer (sequence = pos + 1)
                slot.sequence.store(pos +% 1, .release);
                // _ discard old value
                _ = self.push_count.fetchAdd(1, .monotonic);
                return true;
            } else if (diff < 0) {
                // Buffer is full (consumer hasn't caught up)
                // _ discard old value
                _ = self.failed_pushes.fetchAdd(1, .monotonic);
                return false;
            } else {
                // Another producer is ahead, reload head position
                pos = self.head.load(.monotonic);
            }
        }
    }

    // Push with spin-wait retry on full buffer (bounded retries)
    pub fn pushWait(self: *LockFreeBuffer, point: DataPoint, max_retries: usize) bool {
        var retries: usize = 0;
        while (retries < max_retries) {
            if (self.push(point)) return true;
            // Brief pause to let consumer catch up
            std.atomic.spinLoopHint();
            retries += 1;
        }
        return false;
    }

    // Pop a data point (single consumer only, NOT thread-safe for multiple consumers)
    pub fn pop(self: *LockFreeBuffer) ?DataPoint {
        const pos = self.tail.load(.monotonic);
        const slot = &self.buffer[pos & self.mask];
        const seq = slot.sequence.load(.acquire);

        // Expected sequence for a ready slot is pos + 1
        const seq_i: isize = @intCast(seq);
        const pos_i: isize = @intCast(pos +% 1);
        const diff = seq_i - pos_i;

        if (diff == 0) {
            // Data is ready to read
            const data = slot.data;
            // Mark slot as available for producers (sequence = pos + capacity)
            slot.sequence.store(pos +% self.capacity, .release);
            self.tail.store(pos +% 1, .monotonic);
            _ = self.pop_count.fetchAdd(1, .monotonic);
            return data;
        }

        // No data ready (diff < 0 means producers haven't written yet)
        return null;
    }

    // Drain multiple items into output slice (single consumer)
    // Returns number of items drained
    pub fn drain(self: *LockFreeBuffer, out: []DataPoint) usize {
        var count: usize = 0;
        while (count < out.len) {
            if (self.pop()) |point| {
                out[count] = point;
                count += 1;
            } else {
                break;
            }
        }
        return count;
    }

    // Drain all available items (allocates result slice)
    pub fn drainAll(self: *LockFreeBuffer, allocator: Allocator) ![]DataPoint {
        var result = std.ArrayList(DataPoint){};
        errdefer result.deinit(allocator);

        while (self.pop()) |point| {
            try result.append(allocator, point);
        }

        return result.toOwnedSlice(allocator);
    }

    // Number of items currently in buffer (approximate, may be stale)
    pub fn len(self: *const LockFreeBuffer) usize {
        const head = self.head.load(.monotonic);
        const tail = self.tail.load(.monotonic);
        return head -% tail;
    }

    // Check if buffer is empty (approximate)
    pub fn isEmpty(self: *const LockFreeBuffer) bool {
        return self.len() == 0;
    }

    // Check if buffer is full (approximate)
    pub fn isFull(self: *const LockFreeBuffer) bool {
        return self.len() >= self.capacity;
    }

    // Get stats
    pub fn getStats(self: *const LockFreeBuffer) Stats {
        return .{
            .capacity = self.capacity,
            .current_len = self.len(),
            .total_pushes = self.push_count.load(.monotonic),
            .total_pops = self.pop_count.load(.monotonic),
            .failed_pushes = self.failed_pushes.load(.monotonic),
        };
    }

    pub const Stats = struct {
        capacity: usize,
        current_len: usize,
        total_pushes: u64,
        total_pops: u64,
        failed_pushes: u64,
    };
};

// Tests

test "LockFreeBuffer single thread push/pop" {
    const allocator = testing.allocator;

    var buf = try LockFreeBuffer.init(allocator, 8);
    defer buf.deinit();

    // Buffer should be empty
    try testing.expect(buf.isEmpty());
    try testing.expectEqual(@as(usize, 8), buf.capacity);

    // Push some items
    try testing.expect(buf.push(.{ .series_id = 1, .timestamp = 100, .value = 1.0 }));
    try testing.expect(buf.push(.{ .series_id = 1, .timestamp = 200, .value = 2.0 }));
    try testing.expect(buf.push(.{ .series_id = 2, .timestamp = 100, .value = 3.0 }));

    try testing.expectEqual(@as(usize, 3), buf.len());

    // Pop items
    const p1 = buf.pop().?;
    try testing.expectEqual(@as(u64, 1), p1.series_id);
    try testing.expectEqual(@as(i64, 100), p1.timestamp);
    try testing.expectEqual(@as(f64, 1.0), p1.value);

    const p2 = buf.pop().?;
    try testing.expectEqual(@as(i64, 200), p2.timestamp);

    const p3 = buf.pop().?;
    try testing.expectEqual(@as(u64, 2), p3.series_id);

    // Buffer should be empty again
    try testing.expect(buf.isEmpty());
    try testing.expect(buf.pop() == null);
}

test "LockFreeBuffer full buffer" {
    const allocator = testing.allocator;

    var buf = try LockFreeBuffer.init(allocator, 4); // Power of 2
    defer buf.deinit();

    // Fill buffer
    for (0..4) |i| {
        try testing.expect(buf.push(.{
            .series_id = 1,
            .timestamp = @intCast(i),
            .value = @floatFromInt(i),
        }));
    }

    try testing.expect(buf.isFull());

    // Next push should fail
    try testing.expect(!buf.push(.{ .series_id = 1, .timestamp = 999, .value = 0 }));

    // Pop one and push should succeed
    _ = buf.pop();
    try testing.expect(buf.push(.{ .series_id = 1, .timestamp = 999, .value = 0 }));
}

test "LockFreeBuffer drain" {
    const allocator = testing.allocator;

    var buf = try LockFreeBuffer.init(allocator, 16);
    defer buf.deinit();

    // Push 10 items
    for (0..10) |i| {
        _ = buf.push(.{
            .series_id = 1,
            .timestamp = @intCast(i * 1000),
            .value = @floatFromInt(i),
        });
    }

    // Drain into fixed buffer
    var out: [5]DataPoint = undefined;
    const count = buf.drain(&out);
    try testing.expectEqual(@as(usize, 5), count);
    try testing.expectEqual(@as(i64, 0), out[0].timestamp);
    try testing.expectEqual(@as(i64, 4000), out[4].timestamp);

    // Drain remaining
    const remaining = try buf.drainAll(allocator);
    defer allocator.free(remaining);
    try testing.expectEqual(@as(usize, 5), remaining.len);
    try testing.expectEqual(@as(i64, 5000), remaining[0].timestamp);
}

test "LockFreeBuffer concurrent producers" {
    const allocator = testing.allocator;

    var buf = try LockFreeBuffer.init(allocator, 1024);
    defer buf.deinit();

    const num_threads = 4;
    const items_per_thread = 100;

    // Spawn producer threads
    var threads: [num_threads]std.Thread = undefined;
    for (0..num_threads) |t| {
        threads[t] = try std.Thread.spawn(.{}, struct {
            fn produce(buffer: *LockFreeBuffer, thread_id: usize) void {
                for (0..items_per_thread) |i| {
                    const point = DataPoint{
                        .series_id = thread_id,
                        .timestamp = @intCast(i),
                        .value = @floatFromInt(thread_id * 1000 + i),
                    };
                    // Keep trying until success
                    while (!buffer.push(point)) {
                        std.atomic.spinLoopHint();
                    }
                }
            }
        }.produce, .{ &buf, t });
    }

    // Wait for all producers to finish
    for (&threads) |*t| {
        t.join();
    }

    // Consumer: drain all items
    var consumed: usize = 0;
    const expected = num_threads * items_per_thread;

    while (buf.pop()) |_| {
        consumed += 1;
    }

    try testing.expectEqual(expected, consumed);

    // Verify stats
    const stats = buf.getStats();
    try testing.expectEqual(@as(u64, expected), stats.total_pushes);
    try testing.expectEqual(@as(u64, expected), stats.total_pops);
}

test "LockFreeBuffer capacity rounds to power of 2" {
    const allocator = testing.allocator;

    var buf = try LockFreeBuffer.init(allocator, 10);
    defer buf.deinit();

    try testing.expectEqual(@as(usize, 16), buf.capacity);
    try testing.expectEqual(@as(usize, 15), buf.mask);
}
