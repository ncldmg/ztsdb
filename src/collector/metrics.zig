const std = @import("std");
const collector = @import("collector.zig");
const series = @import("series.zig");

const CgroupMetrics = collector.CgroupMetrics;
const MetricType = series.MetricType;
const Allocator = std.mem.Allocator;

// Calculated rate metrics for a container
pub const RateMetrics = struct {
    // CPU usage as percentage (0-100 * num_cpus)
    cpu_percent: f64,
    // Memory usage in bytes (absolute, not a rate)
    memory_bytes: u64,
    // Network receive rate in bytes/sec
    net_rx_bytes_per_sec: f64,
    // Network transmit rate in bytes/sec
    net_tx_bytes_per_sec: f64,
    // Network receive rate in packets/sec
    net_rx_packets_per_sec: f64,
    // Network transmit rate in packets/sec
    net_tx_packets_per_sec: f64,
    // Block read rate in bytes/sec
    block_read_bytes_per_sec: f64,
    // Block write rate in bytes/sec
    block_write_bytes_per_sec: f64,
    // Block read rate in ops/sec
    block_read_ops_per_sec: f64,
    // Block write rate in ops/sec
    block_write_ops_per_sec: f64,
    // Timestamp of this sample (nanoseconds)
    timestamp_ns: u64,

    pub fn zero() RateMetrics {
        return .{
            .cpu_percent = 0,
            .memory_bytes = 0,
            .net_rx_bytes_per_sec = 0,
            .net_tx_bytes_per_sec = 0,
            .net_rx_packets_per_sec = 0,
            .net_tx_packets_per_sec = 0,
            .block_read_bytes_per_sec = 0,
            .block_write_bytes_per_sec = 0,
            .block_read_ops_per_sec = 0,
            .block_write_ops_per_sec = 0,
            .timestamp_ns = 0,
        };
    }

    // Get a specific metric value by type
    pub fn getValue(self: *const RateMetrics, metric: MetricType) f64 {
        return switch (metric) {
            .cpu_usage => self.cpu_percent,
            .memory_bytes => @floatFromInt(self.memory_bytes),
            .net_rx_bytes => self.net_rx_bytes_per_sec,
            .net_tx_bytes => self.net_tx_bytes_per_sec,
            .net_rx_packets => self.net_rx_packets_per_sec,
            .net_tx_packets => self.net_tx_packets_per_sec,
            .block_read_bytes => self.block_read_bytes_per_sec,
            .block_write_bytes => self.block_write_bytes_per_sec,
            .block_read_ops => self.block_read_ops_per_sec,
            .block_write_ops => self.block_write_ops_per_sec,
        };
    }
};

// Previous sample state for delta calculation
pub const PreviousSample = struct {
    metrics: CgroupMetrics,
    timestamp_ns: i128,
};

// Rate calculator that tracks previous samples
pub const RateCalculator = struct {
    allocator: Allocator,
    // Previous samples by cgroup ID
    previous: std.AutoHashMap(u64, PreviousSample),
    // Minimum time delta for rate calculation (avoid division by tiny values)
    min_delta_ns: u64 = 100_000_000, // 100ms

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .previous = std.AutoHashMap(u64, PreviousSample).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.previous.deinit();
    }

    // Calculate rates from raw metrics
    // Returns null if this is the first sample or time delta is too small
    pub fn calculate(self: *Self, cgroup_id: u64, current: *const CgroupMetrics) ?RateMetrics {
        const now_ns = std.time.nanoTimestamp();

        // Get previous sample
        const prev_opt = self.previous.get(cgroup_id);

        // Store current as previous for next time
        self.previous.put(cgroup_id, .{
            .metrics = current.*,
            .timestamp_ns = now_ns,
        }) catch {
            // If we can't store, we'll just recalculate next time
        };

        // First sample - no rate possible
        const prev = prev_opt orelse return null;

        // Calculate time delta
        const time_delta_ns = now_ns - prev.timestamp_ns;
        if (time_delta_ns < self.min_delta_ns) {
            return null; // Too short interval
        }

        const time_delta_sec: f64 = @as(f64, @floatFromInt(time_delta_ns)) / 1_000_000_000.0;

        // Calculate deltas (handle counter wraps by treating negative as reset)
        const cpu_delta = safeDelta(current.cpu_time_ns, prev.metrics.cpu_time_ns);
        const net_rx_bytes_delta = safeDelta(current.net_rx_bytes, prev.metrics.net_rx_bytes);
        const net_tx_bytes_delta = safeDelta(current.net_tx_bytes, prev.metrics.net_tx_bytes);
        const net_rx_packets_delta = safeDelta(current.net_rx_packets, prev.metrics.net_rx_packets);
        const net_tx_packets_delta = safeDelta(current.net_tx_packets, prev.metrics.net_tx_packets);
        const block_read_bytes_delta = safeDelta(current.block_read_bytes, prev.metrics.block_read_bytes);
        const block_write_bytes_delta = safeDelta(current.block_write_bytes, prev.metrics.block_write_bytes);
        const block_read_ops_delta = safeDelta(current.block_read_ops, prev.metrics.block_read_ops);
        const block_write_ops_delta = safeDelta(current.block_write_ops, prev.metrics.block_write_ops);

        // CPU percentage: (ns used / ns elapsed) * 100
        // Note: Can exceed 100% on multi-core systems
        const cpu_percent = (@as(f64, @floatFromInt(cpu_delta)) / @as(f64, @floatFromInt(time_delta_ns))) * 100.0;

        return RateMetrics{
            .cpu_percent = cpu_percent,
            .memory_bytes = current.memory_bytes, // Not a rate, just current value
            .net_rx_bytes_per_sec = @as(f64, @floatFromInt(net_rx_bytes_delta)) / time_delta_sec,
            .net_tx_bytes_per_sec = @as(f64, @floatFromInt(net_tx_bytes_delta)) / time_delta_sec,
            .net_rx_packets_per_sec = @as(f64, @floatFromInt(net_rx_packets_delta)) / time_delta_sec,
            .net_tx_packets_per_sec = @as(f64, @floatFromInt(net_tx_packets_delta)) / time_delta_sec,
            .block_read_bytes_per_sec = @as(f64, @floatFromInt(block_read_bytes_delta)) / time_delta_sec,
            .block_write_bytes_per_sec = @as(f64, @floatFromInt(block_write_bytes_delta)) / time_delta_sec,
            .block_read_ops_per_sec = @as(f64, @floatFromInt(block_read_ops_delta)) / time_delta_sec,
            .block_write_ops_per_sec = @as(f64, @floatFromInt(block_write_ops_delta)) / time_delta_sec,
            .timestamp_ns = current.timestamp,
        };
    }

    // Remove tracking for a container (when it's removed)
    pub fn remove(self: *Self, cgroup_id: u64) void {
        _ = self.previous.remove(cgroup_id);
    }

    // Clear all previous samples
    pub fn clear(self: *Self) void {
        self.previous.clearRetainingCapacity();
    }
};

// Calculate delta handling counter wrap/reset
fn safeDelta(current: u64, previous: u64) u64 {
    if (current >= previous) {
        return current - previous;
    }
    // Counter wrapped or reset - treat as reset and use current value
    return current;
}

// Simple rate calculation without tracking (for one-shot calculations)
pub fn calculateRate(
    current: *const CgroupMetrics,
    previous: *const CgroupMetrics,
    time_delta_ns: u64,
) RateMetrics {
    if (time_delta_ns == 0) {
        return RateMetrics.zero();
    }

    const time_delta_sec: f64 = @as(f64, @floatFromInt(time_delta_ns)) / 1_000_000_000.0;

    const cpu_delta = safeDelta(current.cpu_time_ns, previous.cpu_time_ns);
    const cpu_percent = (@as(f64, @floatFromInt(cpu_delta)) / @as(f64, @floatFromInt(time_delta_ns))) * 100.0;

    return RateMetrics{
        .cpu_percent = cpu_percent,
        .memory_bytes = current.memory_bytes,
        .net_rx_bytes_per_sec = @as(f64, @floatFromInt(safeDelta(current.net_rx_bytes, previous.net_rx_bytes))) / time_delta_sec,
        .net_tx_bytes_per_sec = @as(f64, @floatFromInt(safeDelta(current.net_tx_bytes, previous.net_tx_bytes))) / time_delta_sec,
        .net_rx_packets_per_sec = @as(f64, @floatFromInt(safeDelta(current.net_rx_packets, previous.net_rx_packets))) / time_delta_sec,
        .net_tx_packets_per_sec = @as(f64, @floatFromInt(safeDelta(current.net_tx_packets, previous.net_tx_packets))) / time_delta_sec,
        .block_read_bytes_per_sec = @as(f64, @floatFromInt(safeDelta(current.block_read_bytes, previous.block_read_bytes))) / time_delta_sec,
        .block_write_bytes_per_sec = @as(f64, @floatFromInt(safeDelta(current.block_write_bytes, previous.block_write_bytes))) / time_delta_sec,
        .block_read_ops_per_sec = @as(f64, @floatFromInt(safeDelta(current.block_read_ops, previous.block_read_ops))) / time_delta_sec,
        .block_write_ops_per_sec = @as(f64, @floatFromInt(safeDelta(current.block_write_ops, previous.block_write_ops))) / time_delta_sec,
        .timestamp_ns = current.timestamp,
    };
}

test "safe delta" {
    // Normal case
    try std.testing.expectEqual(@as(u64, 100), safeDelta(200, 100));

    // Counter wrap/reset
    try std.testing.expectEqual(@as(u64, 50), safeDelta(50, 100));

    // No change
    try std.testing.expectEqual(@as(u64, 0), safeDelta(100, 100));
}

test "rate calculator" {
    const allocator = std.testing.allocator;
    var calc = RateCalculator.init(allocator);
    defer calc.deinit();

    var m1 = CgroupMetrics.zero();
    m1.cpu_time_ns = 1_000_000_000; // 1 second
    m1.net_rx_bytes = 1000;

    // First sample - no rate
    try std.testing.expect(calc.calculate(1, &m1) == null);

    // Wait a bit and take another sample
    // In real tests we'd wait, but we can't easily test timing
    // So we just verify the state was stored
    try std.testing.expect(calc.previous.contains(1));
}

test "rate metrics get value" {
    var rates = RateMetrics.zero();
    rates.cpu_percent = 50.0;
    rates.memory_bytes = 1024;

    try std.testing.expectEqual(@as(f64, 50.0), rates.getValue(.cpu_usage));
    try std.testing.expectEqual(@as(f64, 1024.0), rates.getValue(.memory_bytes));
}
