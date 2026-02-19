const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

// Binary protocol for time series data ingestion
//
// Message format:
// ┌────────────────────────────────────────────────────────┐
// │ Header (16 bytes)                                      │
// │ ┌──────────┬─────────┬──────────┬────────────────────┐ │
// │ │ Magic    │ Version │ MsgType  │ Payload Length     │ │
// │ │ 4 bytes  │ 1 byte  │ 1 byte   │ 4 bytes            │ │
// │ └──────────┴─────────┴──────────┴────────────────────┘ │
// │ Reserved (6 bytes)                                     │
// ├────────────────────────────────────────────────────────┤
// │ Payload (variable)                                     │
// └────────────────────────────────────────────────────────┘
//
// All integers are little-endian.

pub const MAGIC: [4]u8 = .{ 'Z', 'T', 'S', 'D' };
pub const VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 16;
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024; // 16MB max payload
pub const DATA_POINT_SIZE: usize = 24; // 8 + 8 + 8 bytes

pub const MessageType = enum(u8) {
    // Write operations
    insert = 0x01,
    insert_batch = 0x02,

    // Read operations
    query = 0x10,
    query_latest = 0x11,
    query_stats = 0x12,

    // Control
    ping = 0x20,
    flush = 0x21,
    sync = 0x22,

    // Responses
    ok = 0x80,
    error_response = 0x81,
    data_response = 0x82,
    stats_response = 0x83,

    _,
};

pub const Header = struct {
    magic: [4]u8,
    version: u8,
    msg_type: MessageType,
    payload_len: u32,
    _reserved: [6]u8 = .{ 0, 0, 0, 0, 0, 0 },

    pub fn encode(self: Header, buf: *[HEADER_SIZE]u8) void {
        @memcpy(buf[0..4], &self.magic);
        buf[4] = self.version;
        buf[5] = @intFromEnum(self.msg_type);
        std.mem.writeInt(u32, buf[6..10], self.payload_len, .little);
        @memcpy(buf[10..16], &self._reserved);
    }

    pub fn decode(buf: *const [HEADER_SIZE]u8) DecodeError!Header {
        const magic = buf[0..4].*;
        if (!std.mem.eql(u8, &magic, &MAGIC)) {
            return error.InvalidMagic;
        }

        const version = buf[4];
        if (version != VERSION) {
            return error.UnsupportedVersion;
        }

        const payload_len = std.mem.readInt(u32, buf[6..10], .little);
        if (payload_len > MAX_PAYLOAD_SIZE) {
            return error.PayloadTooLarge;
        }

        return Header{
            .magic = magic,
            .version = version,
            .msg_type = @enumFromInt(buf[5]),
            .payload_len = payload_len,
            ._reserved = buf[10..16].*,
        };
    }
};

// Data point for wire format (24 bytes, packed)
pub const WireDataPoint = extern struct {
    series_id: u64 align(1),
    timestamp: i64 align(1),
    value: u64 align(1), // f64 as bits

    pub fn fromDataPoint(dp: DataPoint) WireDataPoint {
        return .{
            .series_id = dp.series_id,
            .timestamp = dp.timestamp,
            .value = @bitCast(dp.value),
        };
    }

    pub fn toDataPoint(self: WireDataPoint) DataPoint {
        return .{
            .series_id = self.series_id,
            .timestamp = self.timestamp,
            .value = @bitCast(self.value),
        };
    }
};

pub const DataPoint = struct {
    series_id: u64,
    timestamp: i64,
    value: f64,
};

// Query request payload
pub const QueryRequest = struct {
    series_id: u64,
    start_ts: i64,
    end_ts: i64,

    pub const SIZE: usize = 24;

    pub fn encode(self: QueryRequest, buf: *[SIZE]u8) void {
        std.mem.writeInt(u64, buf[0..8], self.series_id, .little);
        std.mem.writeInt(i64, buf[8..16], self.start_ts, .little);
        std.mem.writeInt(i64, buf[16..24], self.end_ts, .little);
    }

    pub fn decode(buf: *const [SIZE]u8) QueryRequest {
        return .{
            .series_id = std.mem.readInt(u64, buf[0..8], .little),
            .start_ts = std.mem.readInt(i64, buf[8..16], .little),
            .end_ts = std.mem.readInt(i64, buf[16..24], .little),
        };
    }
};

// Series stats response payload
pub const StatsResponse = struct {
    count: u64,
    min_value: u64, // f64 as bits
    max_value: u64, // f64 as bits
    avg_value: u64, // f64 as bits
    min_timestamp: i64,
    max_timestamp: i64,

    pub const SIZE: usize = 48;

    pub fn encode(self: StatsResponse, buf: *[SIZE]u8) void {
        std.mem.writeInt(u64, buf[0..8], self.count, .little);
        std.mem.writeInt(u64, buf[8..16], self.min_value, .little);
        std.mem.writeInt(u64, buf[16..24], self.max_value, .little);
        std.mem.writeInt(u64, buf[24..32], self.avg_value, .little);
        std.mem.writeInt(i64, buf[32..40], self.min_timestamp, .little);
        std.mem.writeInt(i64, buf[40..48], self.max_timestamp, .little);
    }

    pub fn decode(buf: *const [SIZE]u8) StatsResponse {
        return .{
            .count = std.mem.readInt(u64, buf[0..8], .little),
            .min_value = std.mem.readInt(u64, buf[8..16], .little),
            .max_value = std.mem.readInt(u64, buf[16..24], .little),
            .avg_value = std.mem.readInt(u64, buf[24..32], .little),
            .min_timestamp = std.mem.readInt(i64, buf[32..40], .little),
            .max_timestamp = std.mem.readInt(i64, buf[40..48], .little),
        };
    }
};

// Global database stats response
pub const GlobalStatsResponse = struct {
    series_count: u32,
    total_points: u64,
    uptime_seconds: u64,
    connected_clients: u32,

    pub const SIZE: usize = 24;

    pub fn encode(self: GlobalStatsResponse, buf: *[SIZE]u8) void {
        std.mem.writeInt(u32, buf[0..4], self.series_count, .little);
        std.mem.writeInt(u64, buf[4..12], self.total_points, .little);
        std.mem.writeInt(u64, buf[12..20], self.uptime_seconds, .little);
        std.mem.writeInt(u32, buf[20..24], self.connected_clients, .little);
    }

    pub fn decode(buf: *const [SIZE]u8) GlobalStatsResponse {
        return .{
            .series_count = std.mem.readInt(u32, buf[0..4], .little),
            .total_points = std.mem.readInt(u64, buf[4..12], .little),
            .uptime_seconds = std.mem.readInt(u64, buf[12..20], .little),
            .connected_clients = std.mem.readInt(u32, buf[20..24], .little),
        };
    }
};

pub const ErrorCode = enum(u32) {
    none = 0,
    invalid_message = 1,
    series_not_found = 2,
    chunk_full = 3,
    internal_error = 4,
    invalid_payload = 5,
    _,
};

pub const DecodeError = error{
    InvalidMagic,
    UnsupportedVersion,
    PayloadTooLarge,
    InvalidPayloadSize,
    BufferTooSmall,
};

// Protocol encoder/decoder
pub const Protocol = struct {
    allocator: Allocator,
    read_buf: std.ArrayList(u8),
    write_buf: std.ArrayList(u8),

    pub fn init(allocator: Allocator) Protocol {
        return .{
            .allocator = allocator,
            .read_buf = std.ArrayList(u8){},
            .write_buf = std.ArrayList(u8){},
        };
    }

    pub fn deinit(self: *Protocol) void {
        self.read_buf.deinit(self.allocator);
        self.write_buf.deinit(self.allocator);
    }

    // Encode a single insert message
    pub fn encodeInsert(self: *Protocol, dp: DataPoint) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + DATA_POINT_SIZE);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .insert,
            .payload_len = DATA_POINT_SIZE,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        const wire = WireDataPoint.fromDataPoint(dp);
        try self.write_buf.appendSlice(self.allocator, std.mem.asBytes(&wire));

        return self.write_buf.items;
    }

    // Encode a batch insert message
    pub fn encodeInsertBatch(self: *Protocol, points: []const DataPoint) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        const payload_len: u32 = @intCast(points.len * DATA_POINT_SIZE);
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + payload_len);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .insert_batch,
            .payload_len = payload_len,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        for (points) |dp| {
            const wire = WireDataPoint.fromDataPoint(dp);
            try self.write_buf.appendSlice(self.allocator, std.mem.asBytes(&wire));
        }

        return self.write_buf.items;
    }

    // Encode a query request
    pub fn encodeQuery(self: *Protocol, series_id: u64, start_ts: i64, end_ts: i64) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + QueryRequest.SIZE);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .query,
            .payload_len = QueryRequest.SIZE,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        const query = QueryRequest{
            .series_id = series_id,
            .start_ts = start_ts,
            .end_ts = end_ts,
        };
        var query_buf: [QueryRequest.SIZE]u8 = undefined;
        query.encode(&query_buf);
        try self.write_buf.appendSlice(self.allocator, &query_buf);

        return self.write_buf.items;
    }

    // Encode a query latest request (payload is just series_id)
    pub fn encodeQueryLatest(self: *Protocol, series_id: u64) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + 8);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .query_latest,
            .payload_len = 8,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        var id_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &id_buf, series_id, .little);
        try self.write_buf.appendSlice(self.allocator, &id_buf);

        return self.write_buf.items;
    }

    // Encode an OK response
    pub fn encodeOk(self: *Protocol) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .ok,
            .payload_len = 0,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        return self.write_buf.items;
    }

    // Encode an error response
    pub fn encodeError(self: *Protocol, code: ErrorCode) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + 4);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .error_response,
            .payload_len = 4,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        var code_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &code_buf, @intFromEnum(code), .little);
        try self.write_buf.appendSlice(self.allocator, &code_buf);

        return self.write_buf.items;
    }

    // Encode a data response (for query results)
    pub fn encodeDataResponse(self: *Protocol, points: []const DataPoint) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        const payload_len: u32 = @intCast(points.len * DATA_POINT_SIZE);
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + payload_len);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .data_response,
            .payload_len = payload_len,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        for (points) |dp| {
            const wire = WireDataPoint.fromDataPoint(dp);
            try self.write_buf.appendSlice(self.allocator, std.mem.asBytes(&wire));
        }

        return self.write_buf.items;
    }

    // Encode a global stats response
    pub fn encodeGlobalStatsResponse(self: *Protocol, stats: GlobalStatsResponse) ![]const u8 {
        self.write_buf.clearRetainingCapacity();
        try self.write_buf.ensureTotalCapacity(self.allocator, HEADER_SIZE + GlobalStatsResponse.SIZE);

        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .stats_response,
            .payload_len = GlobalStatsResponse.SIZE,
        };

        var header_buf: [HEADER_SIZE]u8 = undefined;
        header.encode(&header_buf);
        try self.write_buf.appendSlice(self.allocator, &header_buf);

        var stats_buf: [GlobalStatsResponse.SIZE]u8 = undefined;
        stats.encode(&stats_buf);
        try self.write_buf.appendSlice(self.allocator, &stats_buf);

        return self.write_buf.items;
    }

    // Decode header from buffer
    pub fn decodeHeader(buf: []const u8) DecodeError!Header {
        if (buf.len < HEADER_SIZE) {
            return error.BufferTooSmall;
        }
        return Header.decode(buf[0..HEADER_SIZE]);
    }

    // Decode data points from payload buffer
    pub fn decodeDataPoints(allocator: Allocator, payload: []const u8) ![]DataPoint {
        if (payload.len % DATA_POINT_SIZE != 0) {
            return error.InvalidPayloadSize;
        }

        const count = payload.len / DATA_POINT_SIZE;
        const points = try allocator.alloc(DataPoint, count);
        errdefer allocator.free(points);

        for (0..count) |i| {
            const offset = i * DATA_POINT_SIZE;
            const wire: *const WireDataPoint = @ptrCast(@alignCast(payload[offset..].ptr));
            points[i] = wire.toDataPoint();
        }

        return points;
    }
};

// Tests

test "Header encode/decode roundtrip" {
    const header = Header{
        .magic = MAGIC,
        .version = VERSION,
        .msg_type = .insert_batch,
        .payload_len = 1024,
    };

    var buf: [HEADER_SIZE]u8 = undefined;
    header.encode(&buf);

    const decoded = try Header.decode(&buf);
    try testing.expectEqualSlices(u8, &MAGIC, &decoded.magic);
    try testing.expectEqual(VERSION, decoded.version);
    try testing.expectEqual(MessageType.insert_batch, decoded.msg_type);
    try testing.expectEqual(@as(u32, 1024), decoded.payload_len);
}

test "Header decode invalid magic" {
    var buf: [HEADER_SIZE]u8 = .{ 'B', 'A', 'D', '!', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    const result = Header.decode(&buf);
    try testing.expectError(error.InvalidMagic, result);
}

test "Header decode unsupported version" {
    var buf: [HEADER_SIZE]u8 = undefined;
    @memcpy(buf[0..4], &MAGIC);
    buf[4] = 99; // Invalid version
    const result = Header.decode(&buf);
    try testing.expectError(error.UnsupportedVersion, result);
}

test "WireDataPoint roundtrip" {
    const dp = DataPoint{
        .series_id = 12345,
        .timestamp = 1699900000000,
        .value = 3.14159,
    };

    const wire = WireDataPoint.fromDataPoint(dp);
    const back = wire.toDataPoint();

    try testing.expectEqual(dp.series_id, back.series_id);
    try testing.expectEqual(dp.timestamp, back.timestamp);
    try testing.expectEqual(dp.value, back.value);
}

test "Protocol encode/decode insert" {
    const allocator = testing.allocator;
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    const dp = DataPoint{
        .series_id = 1,
        .timestamp = 1000000,
        .value = 42.5,
    };

    const encoded = try proto.encodeInsert(dp);
    try testing.expectEqual(HEADER_SIZE + DATA_POINT_SIZE, encoded.len);

    const header = try Protocol.decodeHeader(encoded);
    try testing.expectEqual(MessageType.insert, header.msg_type);
    try testing.expectEqual(@as(u32, DATA_POINT_SIZE), header.payload_len);

    const payload = encoded[HEADER_SIZE..];
    const points = try Protocol.decodeDataPoints(allocator, payload);
    defer allocator.free(points);

    try testing.expectEqual(@as(usize, 1), points.len);
    try testing.expectEqual(dp.series_id, points[0].series_id);
    try testing.expectEqual(dp.timestamp, points[0].timestamp);
    try testing.expectEqual(dp.value, points[0].value);
}

test "Protocol encode/decode batch" {
    const allocator = testing.allocator;
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    const points = [_]DataPoint{
        .{ .series_id = 1, .timestamp = 1000, .value = 10.0 },
        .{ .series_id = 1, .timestamp = 2000, .value = 20.0 },
        .{ .series_id = 2, .timestamp = 1000, .value = 100.0 },
    };

    const encoded = try proto.encodeInsertBatch(&points);
    try testing.expectEqual(HEADER_SIZE + 3 * DATA_POINT_SIZE, encoded.len);

    const header = try Protocol.decodeHeader(encoded);
    try testing.expectEqual(MessageType.insert_batch, header.msg_type);
    try testing.expectEqual(@as(u32, 3 * DATA_POINT_SIZE), header.payload_len);

    const payload = encoded[HEADER_SIZE..];
    const decoded = try Protocol.decodeDataPoints(allocator, payload);
    defer allocator.free(decoded);

    try testing.expectEqual(@as(usize, 3), decoded.len);
    for (points, decoded) |expected, actual| {
        try testing.expectEqual(expected.series_id, actual.series_id);
        try testing.expectEqual(expected.timestamp, actual.timestamp);
        try testing.expectEqual(expected.value, actual.value);
    }
}

test "Protocol query request" {
    const allocator = testing.allocator;
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    const encoded = try proto.encodeQuery(42, 1000, 2000);
    const header = try Protocol.decodeHeader(encoded);

    try testing.expectEqual(MessageType.query, header.msg_type);
    try testing.expectEqual(@as(u32, QueryRequest.SIZE), header.payload_len);

    const query = QueryRequest.decode(encoded[HEADER_SIZE..][0..QueryRequest.SIZE]);
    try testing.expectEqual(@as(u64, 42), query.series_id);
    try testing.expectEqual(@as(i64, 1000), query.start_ts);
    try testing.expectEqual(@as(i64, 2000), query.end_ts);
}

test "Protocol error response" {
    const allocator = testing.allocator;
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    const encoded = try proto.encodeError(.series_not_found);
    const header = try Protocol.decodeHeader(encoded);

    try testing.expectEqual(MessageType.error_response, header.msg_type);
    try testing.expectEqual(@as(u32, 4), header.payload_len);

    const code = std.mem.readInt(u32, encoded[HEADER_SIZE..][0..4], .little);
    try testing.expectEqual(@intFromEnum(ErrorCode.series_not_found), code);
}
