const std = @import("std");
const Allocator = std.mem.Allocator;

// Source of the metric
pub const MetricSource = enum {
    ebpf_container,
    otlp,
    manual,
};

// Information about a registered series
pub const SeriesInfo = struct {
    series_id: u64,
    name: []const u8,
    source: MetricSource,
    // Optional metadata (e.g., container_id for eBPF metrics)
    metadata: ?[]const u8,
};

// Unified metrics registry
pub const Registry = struct {
    allocator: Allocator,
    mutex: std.Thread.Mutex,

    // Map from series name to series ID
    name_to_id: std.StringHashMap(u64),

    // Map from series ID to series info
    id_to_info: std.AutoHashMap(u64, SeriesInfo),

    // Next series ID (for OTLP/manual metrics that don't use hash)
    next_id: std.atomic.Value(u64),

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .mutex = .{},
            .name_to_id = std.StringHashMap(u64).init(allocator),
            .id_to_info = std.AutoHashMap(u64, SeriesInfo).init(allocator),
            .next_id = std.atomic.Value(u64).init(1),
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all allocated strings
        var it = self.id_to_info.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
            if (entry.value_ptr.metadata) |m| {
                self.allocator.free(m);
            }
        }
        self.id_to_info.deinit();

        // Keys in name_to_id point to the same memory as id_to_info.name
        self.name_to_id.deinit();
    }

    // Register a series with a specific ID (used by eBPF collector with hash-based IDs)
    pub fn registerWithId(
        self: *Self,
        series_id: u64,
        name: []const u8,
        source: MetricSource,
        metadata: ?[]const u8,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if already registered
        if (self.id_to_info.contains(series_id)) {
            return; // Already registered
        }

        // Duplicate strings
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);

        const metadata_copy = if (metadata) |m| try self.allocator.dupe(u8, m) else null;
        errdefer if (metadata_copy) |m| self.allocator.free(m);

        // Register
        const info = SeriesInfo{
            .series_id = series_id,
            .name = name_copy,
            .source = source,
            .metadata = metadata_copy,
        };

        try self.id_to_info.put(series_id, info);
        try self.name_to_id.put(name_copy, series_id);
    }

    // Get or create a series ID for a name (used by OTLP receiver)
    pub fn getOrCreateId(self: *Self, name: []const u8, source: MetricSource) !u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if already exists
        if (self.name_to_id.get(name)) |id| {
            return id;
        }

        // Create new ID using FNV-1a hash for consistency
        const series_id = fnvHash(name);

        // Check for hash collision
        if (self.id_to_info.contains(series_id)) {
            // Use incremental ID instead
            const alt_id = self.next_id.fetchAdd(1, .monotonic);
            try self.registerUnlocked(alt_id, name, source, null);
            return alt_id;
        }

        try self.registerUnlocked(series_id, name, source, null);
        return series_id;
    }

    // Internal registration (caller holds mutex)
    fn registerUnlocked(
        self: *Self,
        series_id: u64,
        name: []const u8,
        source: MetricSource,
        metadata: ?[]const u8,
    ) !void {
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);

        const metadata_copy = if (metadata) |m| try self.allocator.dupe(u8, m) else null;
        errdefer if (metadata_copy) |m| self.allocator.free(m);

        const info = SeriesInfo{
            .series_id = series_id,
            .name = name_copy,
            .source = source,
            .metadata = metadata_copy,
        };

        try self.id_to_info.put(series_id, info);
        try self.name_to_id.put(name_copy, series_id);
    }

    // Look up series ID by name
    pub fn getId(self: *Self, name: []const u8) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.name_to_id.get(name);
    }

    // Look up series info by ID
    pub fn getInfo(self: *Self, series_id: u64) ?SeriesInfo {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.id_to_info.get(series_id);
    }

    // Get number of registered series
    pub fn count(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.id_to_info.count();
    }

    // Export all series as JSON
    pub fn toJson(self: *Self, allocator: Allocator) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var json: std.ArrayList(u8) = .{};
        errdefer json.deinit(allocator);

        try json.appendSlice(allocator, "[");

        var first = true;
        var it = self.id_to_info.iterator();
        while (it.next()) |entry| {
            if (!first) try json.appendSlice(allocator, ",");
            first = false;

            const info = entry.value_ptr.*;
            const source_str = switch (info.source) {
                .ebpf_container => "ebpf",
                .otlp => "otlp",
                .manual => "manual",
            };

            const item = if (info.metadata) |meta|
                try std.fmt.allocPrint(
                    allocator,
                    "{{\"id\":{d},\"name\":\"{s}\",\"source\":\"{s}\",\"metadata\":\"{s}\"}}",
                    .{ info.series_id, info.name, source_str, meta },
                )
            else
                try std.fmt.allocPrint(
                    allocator,
                    "{{\"id\":{d},\"name\":\"{s}\",\"source\":\"{s}\"}}",
                    .{ info.series_id, info.name, source_str },
                );
            defer allocator.free(item);
            try json.appendSlice(allocator, item);
        }

        try json.appendSlice(allocator, "]");
        return json.toOwnedSlice(allocator);
    }

    // Filter series by source
    pub fn listBySource(self: *Self, allocator: Allocator, source: MetricSource) ![]SeriesInfo {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result: std.ArrayList(SeriesInfo) = .{};
        errdefer result.deinit(allocator);

        var it = self.id_to_info.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.source == source) {
                try result.append(allocator, entry.value_ptr.*);
            }
        }

        return result.toOwnedSlice(allocator);
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

test "registry basic operations" {
    const allocator = std.testing.allocator;
    var registry = Registry.init(allocator);
    defer registry.deinit();

    // Register via getOrCreateId (OTLP style)
    const id1 = try registry.getOrCreateId("app.requests.total", .otlp);
    const id2 = try registry.getOrCreateId("app.requests.total", .otlp);
    try std.testing.expectEqual(id1, id2);

    // Register with explicit ID (eBPF style)
    try registry.registerWithId(12345, "container.cpu.usage.abc123", .ebpf_container, "abc123");

    // Check count
    try std.testing.expectEqual(@as(usize, 2), registry.count());

    // Lookup
    try std.testing.expectEqual(id1, registry.getId("app.requests.total").?);
    try std.testing.expectEqual(@as(u64, 12345), registry.getId("container.cpu.usage.abc123").?);
}
