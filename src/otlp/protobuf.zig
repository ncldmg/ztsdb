const std = @import("std");

/// Simple Protobuf decoder for OTLP metrics
/// Based on: https://github.com/open-telemetry/opentelemetry-proto
pub const ProtobufDecoder = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) ProtobufDecoder {
        return .{ .data = data, .pos = 0 };
    }

    /// Read a varint from the protobuf stream
    fn readVarint(self: *ProtobufDecoder) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;

        while (self.pos < self.data.len) {
            const byte = self.data[self.pos];
            self.pos += 1;

            result |= @as(u64, byte & 0x7F) << shift;

            if ((byte & 0x80) == 0) {
                return result;
            }

            shift += 7;
            if (shift >= 64) return error.VarintTooLarge;
        }

        return error.UnexpectedEndOfInput;
    }

    /// Read a length-delimited field (string or bytes)
    fn readLengthDelimited(self: *ProtobufDecoder) ![]const u8 {
        const length = try self.readVarint();
        if (self.pos + length > self.data.len) {
            return error.UnexpectedEndOfInput;
        }

        const result = self.data[self.pos .. self.pos + length];
        self.pos += length;
        return result;
    }

    /// Read a fixed 64-bit value
    fn readFixed64(self: *ProtobufDecoder) !u64 {
        if (self.pos + 8 > self.data.len) {
            return error.UnexpectedEndOfInput;
        }

        const result = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return result;
    }

    /// Read a double (64-bit float)
    fn readDouble(self: *ProtobufDecoder) !f64 {
        const bits = try self.readFixed64();
        return @bitCast(bits);
    }

    /// Skip a field based on wire type
    fn skipField(self: *ProtobufDecoder, wire_type: u3) !void {
        switch (wire_type) {
            0 => _ = try self.readVarint(), // Varint
            1 => {
                // Fixed64
                if (self.pos + 8 > self.data.len) return error.UnexpectedEndOfInput;
                self.pos += 8;
            },
            2 => {
                // Length-delimited
                const len = try self.readVarint();
                const len_usize: usize = @intCast(len);
                if (self.pos + len_usize > self.data.len) return error.UnexpectedEndOfInput;
                self.pos += len_usize;
            },
            5 => {
                // Fixed32
                if (self.pos + 4 > self.data.len) return error.UnexpectedEndOfInput;
                self.pos += 4;
            },
            else => {
                std.log.err("Unsupported wire type: {d} at position {d}", .{ wire_type, self.pos });
                // Print some context bytes
                const start = if (self.pos >= 10) self.pos - 10 else 0;
                const end = @min(self.pos + 10, self.data.len);
                std.log.err("Context bytes [{d}..{d}]: ", .{ start, end });
                for (self.data[start..end], start..) |byte, i| {
                    if (i == self.pos - 1) {
                        std.log.err("  >{d:3}: 0x{x:0>2} (tag byte, wire_type={d})", .{ i, byte, wire_type });
                    } else {
                        std.log.err("   {d:3}: 0x{x:0>2}", .{ i, byte });
                    }
                }
                return error.UnsupportedWireType;
            },
        }
    }
};

/// Parsed metric data point
pub const MetricPoint = struct {
    metric_name: []const u8,
    value: f64,
    timestamp: u64,
    container_id: ?[]const u8 = null,
    container_name: ?[]const u8 = null,
};

/// Parse OTLP ExportMetricsServiceRequest
pub fn parseOTLPMetrics(allocator: std.mem.Allocator, data: []const u8) ![]MetricPoint {
    var points: std.ArrayList(MetricPoint) = .{};
    errdefer points.deinit(allocator);

    var decoder = ProtobufDecoder.init(data);

    // Parse top-level ExportMetricsServiceRequest
    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch |err| {
            std.log.warn("Failed to read tag at position {d}: {}", .{ decoder.pos, err });
            break;
        };
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        std.log.debug("ExportMetricsServiceRequest: field={d}, wire_type={d}, pos={d}", .{ field_number, wire_type, decoder.pos });

        if (field_number == 1) {
            // resourceMetrics (repeated)
            const rm_data = try decoder.readLengthDelimited();
            try parseResourceMetrics(allocator, rm_data, &points);
        } else {
            std.log.debug("Skipping unknown field {d} with wire type {d}", .{ field_number, wire_type });
            try decoder.skipField(wire_type);
        }
    }

    return points.toOwnedSlice(allocator);
}

fn parseResourceMetrics(allocator: std.mem.Allocator, data: []const u8, points: *std.ArrayList(MetricPoint)) !void {
    var decoder = ProtobufDecoder.init(data);

    var container_id: ?[]const u8 = null;
    var container_name: ?[]const u8 = null;

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch |err| {
            std.log.warn("ResourceMetrics: Failed to read tag at position {d}: {}", .{ decoder.pos, err });
            break;
        };
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        std.log.debug("ResourceMetrics: field={d}, wire_type={d}, pos={d}", .{ field_number, wire_type, decoder.pos });

        switch (field_number) {
            1 => {
                // resource
                const resource_data = try decoder.readLengthDelimited();
                std.log.debug("  Resource data length: {d}", .{resource_data.len});
                const attrs = try parseResource(allocator, resource_data);
                container_id = attrs.container_id;
                container_name = attrs.container_name;
            },
            2 => {
                // scopeMetrics (repeated)
                const sm_data = try decoder.readLengthDelimited();
                std.log.debug("  ScopeMetrics data length: {d}", .{sm_data.len});
                try parseScopeMetrics(allocator, sm_data, points, container_id, container_name);
            },
            else => {
                std.log.debug("  Skipping unknown ResourceMetrics field {d}", .{field_number});
                try decoder.skipField(wire_type);
            },
        }
    }
}

const ResourceAttrs = struct {
    container_id: ?[]const u8 = null,
    container_name: ?[]const u8 = null,
};

fn parseResource(allocator: std.mem.Allocator, data: []const u8) !ResourceAttrs {
    _ = allocator;
    var decoder = ProtobufDecoder.init(data);
    var attrs = ResourceAttrs{};

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        if (field_number == 1) {
            // attributes (repeated KeyValue)
            const kv_data = try decoder.readLengthDelimited();
            const kv = try parseKeyValue(kv_data);

            if (kv.key) |key| {
                if (std.mem.eql(u8, key, "container.id")) {
                    attrs.container_id = kv.value;
                } else if (std.mem.eql(u8, key, "container.name")) {
                    attrs.container_name = kv.value;
                }
            }
        } else {
            try decoder.skipField(wire_type);
        }
    }

    return attrs;
}

const KeyValue = struct {
    key: ?[]const u8 = null,
    value: ?[]const u8 = null,
};

fn parseKeyValue(data: []const u8) !KeyValue {
    var decoder = ProtobufDecoder.init(data);
    var kv = KeyValue{};

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        switch (field_number) {
            1 => kv.key = try decoder.readLengthDelimited(), // key
            2 => {
                // value (AnyValue)
                const value_data = try decoder.readLengthDelimited();
                kv.value = try parseAnyValue(value_data);
            },
            else => try decoder.skipField(wire_type),
        }
    }

    return kv;
}

fn parseAnyValue(data: []const u8) !?[]const u8 {
    var decoder = ProtobufDecoder.init(data);

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        if (field_number == 1) {
            // string_value
            return try decoder.readLengthDelimited();
        } else {
            try decoder.skipField(wire_type);
        }
    }

    return null;
}

fn parseScopeMetrics(
    allocator: std.mem.Allocator,
    data: []const u8,
    points: *std.ArrayList(MetricPoint),
    container_id: ?[]const u8,
    container_name: ?[]const u8,
) !void {
    var decoder = ProtobufDecoder.init(data);

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch |err| {
            std.log.warn("ScopeMetrics: Failed to read tag at position {d}: {}", .{ decoder.pos, err });
            break;
        };
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        std.log.debug("ScopeMetrics: field={d}, wire_type={d}, pos={d}", .{ field_number, wire_type, decoder.pos });

        if (field_number == 2) {
            // metrics (repeated)
            const metric_data = try decoder.readLengthDelimited();
            std.log.debug("  Metric data length: {d}", .{metric_data.len});
            try parseMetric(allocator, metric_data, points, container_id, container_name);
        } else {
            std.log.debug("  Skipping unknown ScopeMetrics field {d}", .{field_number});
            try decoder.skipField(wire_type);
        }
    }
}

fn parseMetric(
    allocator: std.mem.Allocator,
    data: []const u8,
    points: *std.ArrayList(MetricPoint),
    container_id: ?[]const u8,
    container_name: ?[]const u8,
) !void {
    var decoder = ProtobufDecoder.init(data);

    var metric_name: ?[]const u8 = null;

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        switch (field_number) {
            1 => metric_name = try decoder.readLengthDelimited(), // name
            5 => {
                // gauge
                const gauge_data = try decoder.readLengthDelimited();
                if (metric_name) |name| {
                    try parseGauge(allocator, gauge_data, points, name, container_id, container_name);
                }
            },
            7 => {
                // sum
                const sum_data = try decoder.readLengthDelimited();
                if (metric_name) |name| {
                    try parseSum(allocator, sum_data, points, name, container_id, container_name);
                }
            },
            else => try decoder.skipField(wire_type),
        }
    }
}

fn parseGauge(
    allocator: std.mem.Allocator,
    data: []const u8,
    points: *std.ArrayList(MetricPoint),
    metric_name: []const u8,
    container_id: ?[]const u8,
    container_name: ?[]const u8,
) !void {
    var decoder = ProtobufDecoder.init(data);

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        if (field_number == 1) {
            // dataPoints (repeated)
            const dp_data = try decoder.readLengthDelimited();
            try parseNumberDataPoint(allocator, dp_data, points, metric_name, container_id, container_name);
        } else {
            try decoder.skipField(wire_type);
        }
    }
}

fn parseSum(
    allocator: std.mem.Allocator,
    data: []const u8,
    points: *std.ArrayList(MetricPoint),
    metric_name: []const u8,
    container_id: ?[]const u8,
    container_name: ?[]const u8,
) !void {
    var decoder = ProtobufDecoder.init(data);

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        if (field_number == 1) {
            // dataPoints (repeated)
            const dp_data = try decoder.readLengthDelimited();
            try parseNumberDataPoint(allocator, dp_data, points, metric_name, container_id, container_name);
        } else {
            try decoder.skipField(wire_type);
        }
    }
}

fn parseNumberDataPoint(
    allocator: std.mem.Allocator,
    data: []const u8,
    points: *std.ArrayList(MetricPoint),
    metric_name: []const u8,
    container_id: ?[]const u8,
    container_name: ?[]const u8,
) !void {
    var decoder = ProtobufDecoder.init(data);

    var value: f64 = 0.0;
    var timestamp: u64 = 0;

    while (decoder.pos < decoder.data.len) {
        const tag = decoder.readVarint() catch break;
        const field_number = tag >> 3;
        const wire_type: u3 = @truncate(tag & 0x7);

        switch (field_number) {
            2 => timestamp = try decoder.readFixed64(), // start_time_unix_nano
            3 => timestamp = try decoder.readFixed64(), // time_unix_nano
            4 => value = try decoder.readDouble(), // as_double
            6 => {
                // as_int (sfixed64)
                const int_value = try decoder.readFixed64();
                value = @floatFromInt(@as(i64, @bitCast(int_value)));
            },
            else => try decoder.skipField(wire_type),
        }
    }

    try points.append(allocator, .{
        .metric_name = metric_name,
        .value = value,
        .timestamp = timestamp,
        .container_id = container_id,
        .container_name = container_name,
    });
}
