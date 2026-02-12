const std = @import("std");
const testing = std.testing;
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;

const protocol = @import("../protocol/protocol.zig");
const tsdb_mod = @import("../timeserie/tsdb.zig");

const Header = protocol.Header;
const MessageType = protocol.MessageType;
const Protocol = protocol.Protocol;
const DataPoint = protocol.DataPoint;
const QueryRequest = protocol.QueryRequest;
const ErrorCode = protocol.ErrorCode;
const HEADER_SIZE = protocol.HEADER_SIZE;
const DATA_POINT_SIZE = protocol.DATA_POINT_SIZE;

pub const Server = struct {
    allocator: Allocator,
    address: net.Address,
    listener: ?posix.socket_t,
    tsdb: ?*tsdb_mod.TSDB,
    running: std.atomic.Value(bool),
    proto: Protocol,

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 9876,
        tsdb: ?*tsdb_mod.TSDB = null,
    };

    pub fn init(allocator: Allocator, config: Config) !Server {
        const address = try net.Address.parseIp4(config.host, config.port);

        return Server{
            .allocator = allocator,
            .address = address,
            .listener = null,
            .tsdb = config.tsdb,
            .running = std.atomic.Value(bool).init(false),
            .proto = Protocol.init(allocator),
        };
    }

    pub fn deinit(self: *Server) void {
        self.stop();
        self.proto.deinit();
    }

    pub fn start(self: *Server) !void {
        const sock = try posix.socket(
            posix.AF.INET,
            posix.SOCK.STREAM,
            0,
        );
        errdefer posix.close(sock);

        // Allow address reuse
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        try posix.bind(sock, &self.address.any, @sizeOf(posix.sockaddr.in));
        try posix.listen(sock, 128);

        self.listener = sock;
        self.running.store(true, .release);

        std.log.info("server listening on {any}", .{self.address});
    }

    pub fn stop(self: *Server) void {
        self.running.store(false, .release);
        if (self.listener) |sock| {
            posix.close(sock);
            self.listener = null;
        }
    }

    /// Accept and handle a single connection (blocking)
    pub fn acceptOne(self: *Server) !void {
        const sock = self.listener orelse return error.NotListening;

        var client_addr: posix.sockaddr = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);

        const client_sock = try posix.accept(sock, &client_addr, &addr_len, 0);
        defer posix.close(client_sock);

        try self.handleConnection(client_sock);
    }

    /// Run the server loop (blocking)
    pub fn run(self: *Server) !void {
        try self.start();

        while (self.running.load(.acquire)) {
            self.acceptOne() catch |err| {
                if (err == error.ConnectionResetByPeer or
                    err == error.BrokenPipe)
                {
                    continue;
                }
                if (!self.running.load(.acquire)) break;
                std.log.err("accept error: {}", .{err});
            };
        }
    }

    fn handleConnection(self: *Server, sock: posix.socket_t) !void {
        var header_buf: [HEADER_SIZE]u8 = undefined;
        var payload_buf = std.ArrayList(u8){};
        defer payload_buf.deinit(self.allocator);

        while (self.running.load(.acquire)) {
            // Read header
            const header_read = readExact(sock, &header_buf) catch |err| {
                if (err == error.EndOfFile) return;
                return err;
            };
            if (header_read < HEADER_SIZE) return;

            const header = Protocol.decodeHeader(&header_buf) catch |err| {
                std.log.warn("invalid header: {}", .{err});
                const resp = try self.proto.encodeError(.invalid_message);
                _ = try posix.write(sock, resp);
                continue;
            };

            // Read payload
            payload_buf.clearRetainingCapacity();
            if (header.payload_len > 0) {
                try payload_buf.resize(self.allocator, header.payload_len);
                const payload_read = try readExact(sock, payload_buf.items);
                if (payload_read < header.payload_len) return;
            }

            // Process message
            const response = try self.processMessage(header, payload_buf.items);
            _ = try posix.write(sock, response);
        }
    }

    fn processMessage(self: *Server, header: Header, payload: []const u8) ![]const u8 {
        switch (header.msg_type) {
            .insert => {
                if (payload.len != DATA_POINT_SIZE) {
                    return self.proto.encodeError(.invalid_payload);
                }
                const points = try Protocol.decodeDataPoints(self.allocator, payload);
                defer self.allocator.free(points);

                if (self.tsdb) |db| {
                    const dp = points[0];
                    db.insert(dp.series_id, dp.timestamp, dp.value) catch |err| {
                        std.log.err("insert error: {}", .{err});
                        return self.proto.encodeError(.internal_error);
                    };
                }
                return self.proto.encodeOk();
            },

            .insert_batch => {
                if (payload.len % DATA_POINT_SIZE != 0) {
                    return self.proto.encodeError(.invalid_payload);
                }
                const points = try Protocol.decodeDataPoints(self.allocator, payload);
                defer self.allocator.free(points);

                if (self.tsdb) |db| {
                    // Convert to TSDB DataPoint format
                    const tsdb_points = try self.allocator.alloc(tsdb_mod.DataPoint, points.len);
                    defer self.allocator.free(tsdb_points);

                    for (points, 0..) |p, i| {
                        tsdb_points[i] = .{
                            .series_id = p.series_id,
                            .timestamp = p.timestamp,
                            .value = p.value,
                        };
                    }

                    db.insertBatch(tsdb_points) catch |err| {
                        std.log.err("batch insert error: {}", .{err});
                        return self.proto.encodeError(.internal_error);
                    };
                }
                return self.proto.encodeOk();
            },

            .query => {
                if (payload.len != QueryRequest.SIZE) {
                    return self.proto.encodeError(.invalid_payload);
                }
                const req = QueryRequest.decode(payload[0..QueryRequest.SIZE]);

                if (self.tsdb) |db| {
                    var result = std.ArrayList(tsdb_mod.DataPoint){};
                    defer result.deinit(self.allocator);

                    db.query(req.series_id, req.start_ts, req.end_ts, &result) catch |err| {
                        std.log.err("query error: {}", .{err});
                        return self.proto.encodeError(.internal_error);
                    };

                    // Convert to protocol DataPoint
                    const proto_points = try self.allocator.alloc(DataPoint, result.items.len);
                    defer self.allocator.free(proto_points);

                    for (result.items, 0..) |p, i| {
                        proto_points[i] = .{
                            .series_id = p.series_id,
                            .timestamp = p.timestamp,
                            .value = p.value,
                        };
                    }

                    return self.proto.encodeDataResponse(proto_points);
                }
                return self.proto.encodeDataResponse(&[_]DataPoint{});
            },

            .query_latest => {
                if (payload.len != 8) {
                    return self.proto.encodeError(.invalid_payload);
                }
                const series_id = std.mem.readInt(u64, payload[0..8], .little);

                if (self.tsdb) |db| {
                    if (db.queryLatest(series_id)) |dp| {
                        const proto_point = DataPoint{
                            .series_id = dp.series_id,
                            .timestamp = dp.timestamp,
                            .value = dp.value,
                        };
                        return self.proto.encodeDataResponse(&[_]DataPoint{proto_point});
                    }
                    return self.proto.encodeError(.series_not_found);
                }
                return self.proto.encodeError(.series_not_found);
            },

            .ping => {
                return self.proto.encodeOk();
            },

            .flush => {
                if (self.tsdb) |db| {
                    db.flush() catch |err| {
                        std.log.err("flush error: {}", .{err});
                        return self.proto.encodeError(.internal_error);
                    };
                }
                return self.proto.encodeOk();
            },

            .sync => {
                if (self.tsdb) |db| {
                    db.sync() catch |err| {
                        std.log.err("sync error: {}", .{err});
                        return self.proto.encodeError(.internal_error);
                    };
                }
                return self.proto.encodeOk();
            },

            else => {
                return self.proto.encodeError(.invalid_message);
            },
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

test "Server init and deinit" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 19876,
    });
    defer server.deinit();

    try testing.expect(server.listener == null);
    try testing.expect(!server.running.load(.acquire));
}

test "Server start and stop" {
    const allocator = testing.allocator;

    var server = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 19877,
    });
    defer server.deinit();

    try server.start();
    try testing.expect(server.listener != null);
    try testing.expect(server.running.load(.acquire));

    server.stop();
    try testing.expect(server.listener == null);
    try testing.expect(!server.running.load(.acquire));
}

