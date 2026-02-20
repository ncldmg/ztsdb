const std = @import("std");
const testing = std.testing;
const net = std.net;
const posix = std.posix;
const Allocator = std.mem.Allocator;

const protocol = @import("../protocol/protocol.zig");
const tsdb_mod = @import("../timeserie/tsdb.zig");
const TSDB = tsdb_mod.TSDB;

const Header = protocol.Header;
const MessageType = protocol.MessageType;
const Protocol = protocol.Protocol;
const DataPoint = protocol.DataPoint;
const QueryRequest = protocol.QueryRequest;
const GlobalStatsResponse = protocol.GlobalStatsResponse;
const ErrorCode = protocol.ErrorCode;
const HEADER_SIZE = protocol.HEADER_SIZE;
const DATA_POINT_SIZE = protocol.DATA_POINT_SIZE;

pub const Server = struct {
    allocator: Allocator,
    address: net.Address,
    listener: ?posix.socket_t,
    tsdb: ?*TSDB,
    running: std.atomic.Value(bool),
    active_connections: std.atomic.Value(usize),
    proto: Protocol,
    start_time: i64,

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 9876,
        tsdb: ?*TSDB = null,
    };

    pub fn init(allocator: Allocator, config: Config) !Server {
        const address = try net.Address.parseIp4(config.host, config.port);

        return Server{
            .allocator = allocator,
            .address = address,
            .listener = null,
            .tsdb = config.tsdb,
            .running = std.atomic.Value(bool).init(false),
            .active_connections = std.atomic.Value(usize).init(0),
            .proto = Protocol.init(allocator),
            .start_time = std.time.timestamp(),
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

        // Allow address reuse (avoids TIME_WAIT issues)
        try posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        // Allow port reuse (needed for rapid rebind on Linux)
        posix.setsockopt(sock, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1))) catch {};

        try posix.bind(sock, &self.address.any, @sizeOf(posix.sockaddr.in));
        try posix.listen(sock, 128);

        // Get actual bound address (important when port was 0)
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        try posix.getsockname(sock, &self.address.any, &addr_len);

        self.listener = sock;
        self.running.store(true, .release);

        std.log.info("server listening on port {d}", .{self.address.getPort()});
    }

    pub fn stop(self: *Server) void {
        self.running.store(false, .release);
        if (self.listener) |sock| {
            posix.close(sock);
            self.listener = null;
        }

        // Wait for active connections to finish
        while (self.active_connections.load(.acquire) > 0) {
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }

    // Accept a connection and spawn a thread to handle it
    pub fn acceptOne(self: *Server) !void {
        const sock = self.listener orelse return error.NotListening;

        var client_addr: posix.sockaddr = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);

        const client_sock = try posix.accept(sock, &client_addr, &addr_len, 0);

        // Spawn detached thread to handle this connection
        const thread = try std.Thread.spawn(.{}, handleConnectionThread, .{ self, client_sock });
        thread.detach();
    }

    fn handleConnectionThread(self: *Server, client_sock: posix.socket_t) void {
        _ = self.active_connections.fetchAdd(1, .acq_rel);
        defer {
            _ = self.active_connections.fetchSub(1, .acq_rel);
            posix.close(client_sock);
        }

        self.handleConnection(client_sock) catch |err| {
            // Ignore expected disconnection errors
            if (err != error.EndOfFile and
                err != error.BrokenPipe and
                err != error.ConnectionResetByPeer)
            {
                std.log.err("connection error: {}", .{err});
            }
        };
    }

    // Run the server loop (blocking)
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
                    db.insertOrError(dp.series_id, dp.timestamp, dp.value) catch |err| {
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

            .query_stats => {
                var series_count: u32 = 0;
                var total_points: u64 = 0;

                if (self.tsdb) |db| {
                    const stats = db.getStats();
                    series_count = stats.series_count;
                    total_points = stats.total_points + stats.buffered_points;
                }

                const now = std.time.timestamp();
                const uptime: u64 = @intCast(@max(0, now - self.start_time));

                return self.proto.encodeGlobalStatsResponse(.{
                    .series_count = series_count,
                    .total_points = total_points,
                    .uptime_seconds = uptime,
                    .connected_clients = @intCast(self.active_connections.load(.acquire)),
                });
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
        .port = 0, // OS assigns free port
    });
    defer server.deinit();

    try testing.expect(server.listener == null);
    try testing.expect(!server.running.load(.acquire));
}

test "Server start and stop" {
    const allocator = testing.allocator;

    var srv = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer srv.deinit();

    try srv.start();
    try testing.expect(srv.listener != null);
    try testing.expect(srv.running.load(.acquire));

    srv.stop();
    try testing.expect(srv.listener == null);
    try testing.expect(!srv.running.load(.acquire));
}

// Helper to run accept loop in background for tests
fn runAcceptLoop(srv: *Server) void {
    const sock = srv.listener orelse return;
    var fds = [_]posix.pollfd{
        .{ .fd = sock, .events = posix.POLL.IN, .revents = 0 },
    };
    while (srv.running.load(.acquire)) {
        const ret = posix.poll(&fds, 50) catch continue;
        if (ret == 0) continue;
        srv.acceptOne() catch |err| {
            if (!srv.running.load(.acquire)) return;
            std.log.err("test accept error: {}", .{err});
        };
    }
}

test "Server ping response" {
    const allocator = testing.allocator;

    var srv = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer srv.deinit();
    try srv.start();

    // Run accept loop in background
    const accept_thread = try std.Thread.spawn(.{}, runAcceptLoop, .{&srv});
    defer {
        srv.stop();
        accept_thread.join();
    }

    // Connect client
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(sock);
    try posix.connect(sock, &srv.address.any, @sizeOf(posix.sockaddr.in));

    // Send ping
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    var header_buf: [HEADER_SIZE]u8 = undefined;
    const ping_header = Header{
        .magic = protocol.MAGIC,
        .version = protocol.VERSION,
        .msg_type = .ping,
        .payload_len = 0,
    };
    ping_header.encode(&header_buf);
    _ = try posix.write(sock, &header_buf);

    // Read response
    var resp_buf: [HEADER_SIZE]u8 = undefined;
    _ = try readExact(sock, &resp_buf);
    const resp_header = try Protocol.decodeHeader(&resp_buf);

    try testing.expectEqual(MessageType.ok, resp_header.msg_type);
}

test "Server insert and query with TSDB" {
    const allocator = testing.allocator;
    const fs = std.fs;

    // Create TSDB
    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_server_tsdb.wal",
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_server_tsdb.wal") catch {};

    try tsdb.start();

    // Create server with TSDB
    var srv = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0,
        .tsdb = &tsdb,
    });
    defer srv.deinit();
    try srv.start();

    // Run accept loop in background
    const accept_thread = try std.Thread.spawn(.{}, runAcceptLoop, .{&srv});
    defer {
        srv.stop();
        accept_thread.join();
    }

    // Connect client
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(sock);
    try posix.connect(sock, &srv.address.any, @sizeOf(posix.sockaddr.in));

    // Send insert
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    const dp = DataPoint{ .series_id = 1, .timestamp = 1000, .value = 42.5 };
    const insert_msg = try proto.encodeInsert(dp);
    _ = try posix.write(sock, insert_msg);

    // Read insert response
    var resp_buf: [HEADER_SIZE]u8 = undefined;
    _ = try readExact(sock, &resp_buf);
    var resp_header = try Protocol.decodeHeader(&resp_buf);
    try testing.expectEqual(MessageType.ok, resp_header.msg_type);

    // Wait for concurrent consumers to process and flush
    std.Thread.sleep(50 * std.time.ns_per_ms);
    try tsdb.flush();

    // Send query
    const query_msg = try proto.encodeQuery(1, 0, 2000);
    _ = try posix.write(sock, query_msg);

    // Read query response header
    _ = try readExact(sock, &resp_buf);
    resp_header = try Protocol.decodeHeader(&resp_buf);
    try testing.expectEqual(MessageType.data_response, resp_header.msg_type);
    try testing.expectEqual(@as(u32, DATA_POINT_SIZE), resp_header.payload_len);

    // Read query response payload
    var payload_buf: [DATA_POINT_SIZE]u8 = undefined;
    _ = try readExact(sock, &payload_buf);
    const points = try Protocol.decodeDataPoints(allocator, &payload_buf);
    defer allocator.free(points);

    try testing.expectEqual(@as(usize, 1), points.len);
    try testing.expectEqual(@as(u64, 1), points[0].series_id);
    try testing.expectEqual(@as(i64, 1000), points[0].timestamp);
    try testing.expectEqual(@as(f64, 42.5), points[0].value);
}

test "Server batch insert" {
    const allocator = testing.allocator;
    const fs = std.fs;

    // Create TSDB
    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_server_batch.wal",
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_server_batch.wal") catch {};

    try tsdb.start();

    // Create server with TSDB
    var srv = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0,
        .tsdb = &tsdb,
    });
    defer srv.deinit();
    try srv.start();

    // Run accept loop in background
    const accept_thread = try std.Thread.spawn(.{}, runAcceptLoop, .{&srv});
    defer {
        srv.stop();
        accept_thread.join();
    }

    // Connect client
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(sock);
    try posix.connect(sock, &srv.address.any, @sizeOf(posix.sockaddr.in));

    // Send batch insert
    var proto = Protocol.init(allocator);
    defer proto.deinit();

    const batch = [_]DataPoint{
        .{ .series_id = 1, .timestamp = 1000, .value = 10.0 },
        .{ .series_id = 1, .timestamp = 2000, .value = 20.0 },
        .{ .series_id = 1, .timestamp = 3000, .value = 30.0 },
    };
    const batch_msg = try proto.encodeInsertBatch(&batch);
    _ = try posix.write(sock, batch_msg);

    // Read response
    var resp_buf: [HEADER_SIZE]u8 = undefined;
    _ = try readExact(sock, &resp_buf);
    var resp_header = try Protocol.decodeHeader(&resp_buf);
    try testing.expectEqual(MessageType.ok, resp_header.msg_type);

    // Wait for concurrent consumers and flush
    std.Thread.sleep(50 * std.time.ns_per_ms);
    try tsdb.flush();

    // Query to verify
    const query_msg = try proto.encodeQuery(1, 0, 5000);
    _ = try posix.write(sock, query_msg);

    // Read query response
    _ = try readExact(sock, &resp_buf);
    resp_header = try Protocol.decodeHeader(&resp_buf);
    try testing.expectEqual(MessageType.data_response, resp_header.msg_type);
    try testing.expectEqual(@as(u32, 3 * DATA_POINT_SIZE), resp_header.payload_len);

    // Read payload
    var payload_buf: [3 * DATA_POINT_SIZE]u8 = undefined;
    _ = try readExact(sock, &payload_buf);
    const points = try Protocol.decodeDataPoints(allocator, &payload_buf);
    defer allocator.free(points);

    try testing.expectEqual(@as(usize, 3), points.len);
    try testing.expectEqual(@as(f64, 10.0), points[0].value);
    try testing.expectEqual(@as(f64, 20.0), points[1].value);
    try testing.expectEqual(@as(f64, 30.0), points[2].value);
}

test "Server query latest" {
    const allocator = testing.allocator;
    const fs = std.fs;

    // Create TSDB
    var tsdb = try TSDB.init(allocator, .{
        .wal_path = "/tmp/test_server_latest.wal",
    });
    defer tsdb.deinit();
    defer fs.cwd().deleteFile("/tmp/test_server_latest.wal") catch {};

    try tsdb.start();

    // Create server with TSDB
    var srv = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0,
        .tsdb = &tsdb,
    });
    defer srv.deinit();
    try srv.start();

    // Run accept loop in background
    const accept_thread = try std.Thread.spawn(.{}, runAcceptLoop, .{&srv});
    defer {
        srv.stop();
        accept_thread.join();
    }

    // Connect client
    const sock = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    defer posix.close(sock);
    try posix.connect(sock, &srv.address.any, @sizeOf(posix.sockaddr.in));

    var proto = Protocol.init(allocator);
    defer proto.deinit();

    // Insert some data
    const batch = [_]DataPoint{
        .{ .series_id = 1, .timestamp = 1000, .value = 10.0 },
        .{ .series_id = 1, .timestamp = 2000, .value = 20.0 },
        .{ .series_id = 1, .timestamp = 3000, .value = 30.0 },
    };
    _ = try posix.write(sock, try proto.encodeInsertBatch(&batch));

    var resp_buf: [HEADER_SIZE]u8 = undefined;
    _ = try readExact(sock, &resp_buf);

    // Wait for concurrent consumers and flush
    std.Thread.sleep(50 * std.time.ns_per_ms);
    try tsdb.flush();

    // Query latest
    const latest_msg = try proto.encodeQueryLatest(1);
    _ = try posix.write(sock, latest_msg);

    // Read response
    _ = try readExact(sock, &resp_buf);
    const resp_header = try Protocol.decodeHeader(&resp_buf);
    try testing.expectEqual(MessageType.data_response, resp_header.msg_type);

    var payload_buf: [DATA_POINT_SIZE]u8 = undefined;
    _ = try readExact(sock, &payload_buf);
    const points = try Protocol.decodeDataPoints(allocator, &payload_buf);
    defer allocator.free(points);

    try testing.expectEqual(@as(usize, 1), points.len);
    try testing.expectEqual(@as(i64, 3000), points[0].timestamp);
    try testing.expectEqual(@as(f64, 30.0), points[0].value);
}
