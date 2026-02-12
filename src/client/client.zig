const std = @import("std");
const testing = std.testing;
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;

const protocol = @import("../protocol/protocol.zig");

const Header = protocol.Header;
const MessageType = protocol.MessageType;
const Protocol = protocol.Protocol;
const DataPoint = protocol.DataPoint;
const QueryRequest = protocol.QueryRequest;
const StatsResponse = protocol.StatsResponse;
const ErrorCode = protocol.ErrorCode;
const HEADER_SIZE = protocol.HEADER_SIZE;
const DATA_POINT_SIZE = protocol.DATA_POINT_SIZE;
const MAGIC = protocol.MAGIC;
const VERSION = protocol.VERSION;

pub const Client = struct {
    allocator: Allocator,
    address: net.Address,
    sock: ?posix.socket_t,
    proto: Protocol,
    recv_buf: std.ArrayList(u8),

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 9876,
    };

    pub const Error = error{
        NotConnected,
        ServerError,
        SeriesNotFound,
        InvalidResponse,
        ConnectionFailed,
    } || Allocator.Error || posix.ReadError || posix.WriteError || protocol.DecodeError;

    pub fn init(allocator: Allocator, config: Config) !Client {
        const address = try net.Address.parseIp4(config.host, config.port);

        return Client{
            .allocator = allocator,
            .address = address,
            .sock = null,
            .proto = Protocol.init(allocator),
            .recv_buf = std.ArrayList(u8){},
        };
    }

    pub fn deinit(self: *Client) void {
        self.disconnect();
        self.proto.deinit();
        self.recv_buf.deinit(self.allocator);
    }

    pub fn connect(self: *Client) !void {
        if (self.sock != null) return;

        const sock = try posix.socket(
            posix.AF.INET,
            posix.SOCK.STREAM,
            0,
        );
        errdefer posix.close(sock);

        try posix.connect(sock, &self.address.any, @sizeOf(posix.sockaddr.in));
        self.sock = sock;
    }

    pub fn disconnect(self: *Client) void {
        if (self.sock) |sock| {
            posix.close(sock);
            self.sock = null;
        }
    }

    pub fn isConnected(self: *const Client) bool {
        return self.sock != null;
    }

    /// Insert a single data point
    pub fn insert(self: *Client, series_id: u64, timestamp: i64, value: f64) !void {
        const msg = try self.proto.encodeInsert(.{
            .series_id = series_id,
            .timestamp = timestamp,
            .value = value,
        });

        try self.sendAndExpectOk(msg);
    }

    /// Insert multiple data points in batch
    pub fn insertBatch(self: *Client, points: []const DataPoint) !void {
        const msg = try self.proto.encodeInsertBatch(points);
        try self.sendAndExpectOk(msg);
    }

    /// Query data points for a series within a time range
    pub fn query(self: *Client, series_id: u64, start_ts: i64, end_ts: i64) ![]DataPoint {
        const msg = try self.proto.encodeQuery(series_id, start_ts, end_ts);
        return self.sendAndReceiveData(msg);
    }

    /// Query the latest value for a series
    pub fn queryLatest(self: *Client, series_id: u64) !?DataPoint {
        const msg = try self.proto.encodeQueryLatest(series_id);

        const sock = self.sock orelse return error.NotConnected;
        _ = try posix.write(sock, msg);

        // Read response header
        var header_buf: [HEADER_SIZE]u8 = undefined;
        _ = try readExact(sock, &header_buf);

        const header = try Protocol.decodeHeader(&header_buf);

        // Read payload
        if (header.payload_len > 0) {
            try self.recv_buf.resize(self.allocator, header.payload_len);
            _ = try readExact(sock, self.recv_buf.items);
        }

        switch (header.msg_type) {
            .data_response => {
                if (header.payload_len == 0) return null;
                const points = try Protocol.decodeDataPoints(self.allocator, self.recv_buf.items[0..header.payload_len]);
                defer self.allocator.free(points);
                if (points.len == 0) return null;
                return points[0];
            },
            .error_response => {
                if (header.payload_len >= 4) {
                    const code: ErrorCode = @enumFromInt(std.mem.readInt(u32, self.recv_buf.items[0..4], .little));
                    if (code == .series_not_found) return null;
                }
                return error.ServerError;
            },
            else => return error.InvalidResponse,
        }
    }

    /// Ping the server
    pub fn ping(self: *Client) !void {
        var msg: [HEADER_SIZE]u8 = undefined;
        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .ping,
            .payload_len = 0,
        };
        header.encode(&msg);

        try self.sendAndExpectOk(&msg);
    }

    /// Request server to flush WAL
    pub fn flush(self: *Client) !void {
        var msg: [HEADER_SIZE]u8 = undefined;
        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .flush,
            .payload_len = 0,
        };
        header.encode(&msg);

        try self.sendAndExpectOk(&msg);
    }

    /// Request server to sync WAL to disk
    pub fn sync(self: *Client) !void {
        var msg: [HEADER_SIZE]u8 = undefined;
        const header = Header{
            .magic = MAGIC,
            .version = VERSION,
            .msg_type = .sync,
            .payload_len = 0,
        };
        header.encode(&msg);

        try self.sendAndExpectOk(&msg);
    }

    fn sendAndExpectOk(self: *Client, msg: []const u8) !void {
        const sock = self.sock orelse return error.NotConnected;

        _ = try posix.write(sock, msg);

        // Read response header
        var header_buf: [HEADER_SIZE]u8 = undefined;
        _ = try readExact(sock, &header_buf);

        const header = try Protocol.decodeHeader(&header_buf);

        // Read any payload (for error responses)
        if (header.payload_len > 0) {
            try self.recv_buf.resize(self.allocator, header.payload_len);
            _ = try readExact(sock, self.recv_buf.items);
        }

        switch (header.msg_type) {
            .ok => return,
            .error_response => return error.ServerError,
            else => return error.InvalidResponse,
        }
    }

    fn sendAndReceiveData(self: *Client, msg: []const u8) ![]DataPoint {
        const sock = self.sock orelse return error.NotConnected;

        _ = try posix.write(sock, msg);

        // Read response header
        var header_buf: [HEADER_SIZE]u8 = undefined;
        _ = try readExact(sock, &header_buf);

        const header = try Protocol.decodeHeader(&header_buf);

        // Read payload
        if (header.payload_len > 0) {
            try self.recv_buf.resize(self.allocator, header.payload_len);
            _ = try readExact(sock, self.recv_buf.items);
        }

        switch (header.msg_type) {
            .data_response => {
                if (header.payload_len == 0) {
                    return try self.allocator.alloc(DataPoint, 0);
                }
                return Protocol.decodeDataPoints(self.allocator, self.recv_buf.items[0..header.payload_len]);
            },
            .error_response => return error.ServerError,
            else => return error.InvalidResponse,
        }
    }
};

fn readExact(sock: posix.socket_t, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const n = posix.read(sock, buf[total..]) catch |err| {
            if (total == 0) return err;
            return total;
        };
        if (n == 0) {
            if (total == 0) return error.EndOfFile;
            return total;
        }
        total += n;
    }
    return total;
}

// Tests

test "Client init and deinit" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{
        .host = "127.0.0.1",
        .port = 19878,
    });
    defer client.deinit();

    try testing.expect(!client.isConnected());
}

test "Client connect to non-existent server fails" {
    const allocator = testing.allocator;

    var client = try Client.init(allocator, .{
        .host = "127.0.0.1",
        .port = 19879, // No server on this port
    });
    defer client.deinit();

    const result = client.connect();
    try testing.expectError(error.ConnectionRefused, result);
}
