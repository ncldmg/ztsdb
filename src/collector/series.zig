const std = @import("std");

const Allocator = std.mem.Allocator;

// Metric types collected for containers
pub const MetricType = enum {
    cpu_usage,
    memory_bytes,
    net_rx_bytes,
    net_tx_bytes,
    net_rx_packets,
    net_tx_packets,
    block_read_bytes,
    block_write_bytes,
    block_read_ops,
    block_write_ops,

    pub fn name(self: MetricType) []const u8 {
        return switch (self) {
            .cpu_usage => "container.cpu.usage",
            .memory_bytes => "container.memory.bytes",
            .net_rx_bytes => "container.net.rx.bytes",
            .net_tx_bytes => "container.net.tx.bytes",
            .net_rx_packets => "container.net.rx.packets",
            .net_tx_packets => "container.net.tx.packets",
            .block_read_bytes => "container.block.read.bytes",
            .block_write_bytes => "container.block.write.bytes",
            .block_read_ops => "container.block.read.ops",
            .block_write_ops => "container.block.write.ops",
        };
    }

    pub fn all() []const MetricType {
        return &[_]MetricType{
            .cpu_usage,
            .memory_bytes,
            .net_rx_bytes,
            .net_tx_bytes,
            .net_rx_packets,
            .net_tx_packets,
            .block_read_bytes,
            .block_write_bytes,
            .block_read_ops,
            .block_write_ops,
        };
    }
};

// FNV-1a hash for deterministic series IDs
pub fn fnvHash(data: []const u8) u64 {
    const FNV_PRIME: u64 = 0x100000001b3;
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;

    var hash: u64 = FNV_OFFSET;
    for (data) |byte| {
        hash ^= byte;
        hash *%= FNV_PRIME;
    }
    return hash;
}

// Generate a series ID from metric type and container ID
pub fn seriesId(metric: MetricType, container_id: []const u8) u64 {
    var hasher = std.hash.Fnv1a_64.init();
    hasher.update(metric.name());
    hasher.update(".");
    hasher.update(container_id);
    return hasher.final();
}

// Generate a series ID from a full metric name
pub fn seriesIdFromName(name: []const u8) u64 {
    return fnvHash(name);
}

// Series registry with caching and reverse lookup
pub const SeriesRegistry = struct {
    allocator: Allocator,
    /// Map from series ID to metric info
    id_to_info: std.AutoHashMap(u64, SeriesInfo),
    /// Map from (cgroup_id, metric_type) to series ID for fast lookup
    cgroup_series: std.AutoHashMap(CgroupMetricKey, u64),

    const Self = @This();

    pub const SeriesInfo = struct {
        series_id: u64,
        metric_type: MetricType,
        container_id: []const u8, // Short ID
        full_name: []const u8, // e.g., "container.cpu.usage.abc123def456"
    };

    const CgroupMetricKey = struct {
        cgroup_id: u64,
        metric_type: MetricType,
    };

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .id_to_info = std.AutoHashMap(u64, SeriesInfo).init(allocator),
            .cgroup_series = std.AutoHashMap(CgroupMetricKey, u64).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.id_to_info.valueIterator();
        while (it.next()) |info| {
            self.allocator.free(info.container_id);
            self.allocator.free(info.full_name);
        }
        self.id_to_info.deinit();
        self.cgroup_series.deinit();
    }

    // Register a container and get series IDs for all its metrics
    pub fn registerContainer(self: *Self, cgroup_id: u64, container_id: []const u8) !void {
        for (MetricType.all()) |metric| {
            _ = try self.getOrCreateSeriesId(cgroup_id, metric, container_id);
        }
    }

    // Get or create a series ID for a container metric
    pub fn getOrCreateSeriesId(
        self: *Self,
        cgroup_id: u64,
        metric: MetricType,
        container_id: []const u8,
    ) !u64 {
        const key = CgroupMetricKey{
            .cgroup_id = cgroup_id,
            .metric_type = metric,
        };

        // Check cache first
        if (self.cgroup_series.get(key)) |id| {
            return id;
        }

        // Generate series ID
        const sid = seriesId(metric, container_id);

        // Check if already registered (collision case)
        if (self.id_to_info.contains(sid)) {
            try self.cgroup_series.put(key, sid);
            return sid;
        }

        // Build full metric name
        const full_name = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}",
            .{ metric.name(), container_id },
        );
        errdefer self.allocator.free(full_name);

        const container_id_copy = try self.allocator.dupe(u8, container_id);
        errdefer self.allocator.free(container_id_copy);

        // Register the series
        const info = SeriesInfo{
            .series_id = sid,
            .metric_type = metric,
            .container_id = container_id_copy,
            .full_name = full_name,
        };

        try self.id_to_info.put(sid, info);
        try self.cgroup_series.put(key, sid);

        return sid;
    }

    // Get series ID for a cgroup metric (fast path)
    pub fn getSeriesId(self: *const Self, cgroup_id: u64, metric: MetricType) ?u64 {
        return self.cgroup_series.get(.{
            .cgroup_id = cgroup_id,
            .metric_type = metric,
        });
    }

    // Get series info by ID
    pub fn getInfo(self: *const Self, series_id: u64) ?SeriesInfo {
        return self.id_to_info.get(series_id);
    }

    // Unregister cgroup mapping for a container (keeps series info for historical queries)
    pub fn unregisterContainer(self: *Self, cgroup_id: u64) void {
        for (MetricType.all()) |metric| {
            const key = CgroupMetricKey{
                .cgroup_id = cgroup_id,
                .metric_type = metric,
            };

            // Only remove the cgroup-to-series mapping, keep series info for historical data
            _ = self.cgroup_series.remove(key);
        }
    }

    // Get the number of registered series
    pub fn count(self: *const Self) usize {
        return self.id_to_info.count();
    }

    // Export all series as JSON array
    pub fn toJson(self: *const Self, allocator: Allocator) ![]u8 {
        var json: std.ArrayList(u8) = .{};
        errdefer json.deinit(allocator);

        try json.appendSlice(allocator, "[");

        var first = true;
        var it = self.id_to_info.iterator();
        while (it.next()) |entry| {
            if (!first) try json.appendSlice(allocator, ",");
            first = false;

            const info = entry.value_ptr.*;
            const item = try std.fmt.allocPrint(
                allocator,
                "{{\"id\":{d},\"name\":\"{s}\",\"container\":\"{s}\"}}",
                .{ info.series_id, info.full_name, info.container_id },
            );
            defer allocator.free(item);
            try json.appendSlice(allocator, item);
        }

        try json.appendSlice(allocator, "]");
        return json.toOwnedSlice(allocator);
    }
};

test "fnv hash consistency" {
    const h1 = fnvHash("container.cpu.usage.abc123");
    const h2 = fnvHash("container.cpu.usage.abc123");
    try std.testing.expectEqual(h1, h2);

    const h3 = fnvHash("container.cpu.usage.def456");
    try std.testing.expect(h1 != h3);
}

test "series id generation" {
    const id1 = seriesId(.cpu_usage, "abc123def456");
    const id2 = seriesId(.cpu_usage, "abc123def456");
    try std.testing.expectEqual(id1, id2);

    const id3 = seriesId(.memory_bytes, "abc123def456");
    try std.testing.expect(id1 != id3);
}

test "series registry" {
    const allocator = std.testing.allocator;
    var registry = SeriesRegistry.init(allocator);
    defer registry.deinit();

    // Register a container
    try registry.registerContainer(12345, "abc123def456");

    // Check all series were created
    try std.testing.expectEqual(@as(usize, 10), registry.count());

    // Verify series IDs are retrievable
    const cpu_id = registry.getSeriesId(12345, .cpu_usage);
    try std.testing.expect(cpu_id != null);

    // Verify info is correct
    const info = registry.getInfo(cpu_id.?);
    try std.testing.expect(info != null);
    try std.testing.expectEqualStrings("abc123def456", info.?.container_id);
    try std.testing.expectEqual(MetricType.cpu_usage, info.?.metric_type);
}
