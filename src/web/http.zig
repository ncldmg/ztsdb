// HTTP server for serving web UI and stats API
const std = @import("std");
const posix = std.posix;

pub const DataPoint = @import("../protocol/protocol.zig").DataPoint;

pub const HttpServer = struct {
    allocator: std.mem.Allocator,
    socket: posix.socket_t,
    port: u16,
    running: std.atomic.Value(bool),
    stats_fn: ?*const fn () Stats,
    query_fn: ?*const fn (std.mem.Allocator, u64, i64, i64) QueryResult,
    web_root: []const u8,

    pub const Stats = struct {
        series_count: u32 = 0,
        total_points: u64 = 0,
        writes_per_sec: f32 = 0,
        wal_size_bytes: u64 = 0,
        uptime_seconds: u64 = 0,
        connected_clients: u32 = 0,
        address: []const u8 = "127.0.0.1:9876",
    };

    pub const QueryResult = struct {
        points: []DataPoint,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *QueryResult) void {
            self.allocator.free(self.points);
        }
    };

    pub fn init(allocator: std.mem.Allocator, port: u16, web_root: []const u8) !HttpServer {
        const socket = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
        errdefer posix.close(socket);

        // Allow address reuse
        posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1))) catch {};

        const addr = posix.sockaddr.in{
            .family = posix.AF.INET,
            .port = std.mem.nativeToBig(u16, port),
            .addr = 0, // INADDR_ANY
        };

        try posix.bind(socket, @ptrCast(&addr), @sizeOf(posix.sockaddr.in));
        try posix.listen(socket, 128);

        // Get actual port if 0 was requested
        var bound_addr: posix.sockaddr.in = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        try posix.getsockname(socket, @ptrCast(&bound_addr), &addr_len);
        const actual_port = std.mem.bigToNative(u16, bound_addr.port);

        return HttpServer{
            .allocator = allocator,
            .socket = socket,
            .port = actual_port,
            .running = std.atomic.Value(bool).init(false),
            .stats_fn = null,
            .query_fn = null,
            .web_root = web_root,
        };
    }

    pub fn deinit(self: *HttpServer) void {
        self.stop();
        posix.close(self.socket);
    }

    pub fn setStatsCallback(self: *HttpServer, callback: *const fn () Stats) void {
        self.stats_fn = callback;
    }

    pub fn setQueryCallback(self: *HttpServer, callback: *const fn (std.mem.Allocator, u64, i64, i64) QueryResult) void {
        self.query_fn = callback;
    }

    pub fn start(self: *HttpServer) !void {
        self.running.store(true, .release);
        const thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
        thread.detach();
    }

    pub fn stop(self: *HttpServer) void {
        self.running.store(false, .release);
    }

    fn acceptLoop(self: *HttpServer) void {
        var pollfds = [_]posix.pollfd{
            .{ .fd = self.socket, .events = posix.POLL.IN, .revents = 0 },
        };

        while (self.running.load(.acquire)) {
            const ready = posix.poll(&pollfds, 100) catch continue;
            if (ready == 0) continue;

            if (pollfds[0].revents & posix.POLL.IN != 0) {
                const client = posix.accept(self.socket, null, null, 0) catch continue;
                const thread = std.Thread.spawn(.{}, handleClient, .{ self, client }) catch {
                    posix.close(client);
                    continue;
                };
                thread.detach();
            }
        }
    }

    fn handleClient(self: *HttpServer, client: posix.socket_t) void {
        defer posix.close(client);

        var buf: [4096]u8 = undefined;
        const n = posix.read(client, &buf) catch return;
        if (n == 0) return;

        const request = buf[0..n];
        self.handleRequest(client, request) catch return;
    }

    fn handleRequest(self: *HttpServer, client: posix.socket_t, request: []const u8) !void {
        // Parse request line
        var lines = std.mem.splitScalar(u8, request, '\n');
        const request_line = lines.next() orelse return;

        var parts = std.mem.splitScalar(u8, request_line, ' ');
        const method = parts.next() orelse return;
        const path = parts.next() orelse return;

        if (!std.mem.eql(u8, method, "GET")) {
            try sendResponse(client, "405 Method Not Allowed", "text/plain", "Method Not Allowed");
            return;
        }

        // Route handling
        if (std.mem.eql(u8, path, "/api/stats")) {
            try self.handleStats(client);
        } else if (std.mem.startsWith(u8, path, "/api/query")) {
            try self.handleQuery(client, path);
        } else {
            try self.handleStatic(client, path);
        }
    }

    fn handleStats(self: *HttpServer, client: posix.socket_t) !void {
        const stats = if (self.stats_fn) |f| f() else Stats{};

        var buf: [512]u8 = undefined;
        const json = try std.fmt.bufPrint(&buf,
            \\{{"series_count":{d},"total_points":{d},"writes_per_sec":{d:.1},"wal_size_bytes":{d},"uptime_seconds":{d},"connected_clients":{d},"address":"{s}"}}
        , .{
            stats.series_count,
            stats.total_points,
            stats.writes_per_sec,
            stats.wal_size_bytes,
            stats.uptime_seconds,
            stats.connected_clients,
            stats.address,
        });

        try sendResponse(client, "200 OK", "application/json", json);
    }

    fn handleQuery(self: *HttpServer, client: posix.socket_t, path: []const u8) !void {
        // Parse query parameters from path: /api/query?series_id=X&start=Y&end=Z
        var series_id: u64 = 0;
        var start_ts: i64 = 0;
        var end_ts: i64 = std.math.maxInt(i64);

        // Find query string
        if (std.mem.indexOf(u8, path, "?")) |qmark| {
            var query_str = path[qmark + 1 ..];
            // Trim carriage return if present
            if (std.mem.indexOfScalar(u8, query_str, '\r')) |cr| {
                query_str = query_str[0..cr];
            }

            var params = std.mem.splitScalar(u8, query_str, '&');
            while (params.next()) |param| {
                if (std.mem.indexOf(u8, param, "=")) |eq| {
                    const key = param[0..eq];
                    const value = param[eq + 1 ..];

                    if (std.mem.eql(u8, key, "series_id")) {
                        series_id = std.fmt.parseInt(u64, value, 10) catch 0;
                    } else if (std.mem.eql(u8, key, "start")) {
                        start_ts = std.fmt.parseInt(i64, value, 10) catch 0;
                    } else if (std.mem.eql(u8, key, "end")) {
                        end_ts = std.fmt.parseInt(i64, value, 10) catch std.math.maxInt(i64);
                    }
                }
            }
        }

        // Execute query
        if (self.query_fn) |query| {
            var result = query(self.allocator, series_id, start_ts, end_ts);
            defer result.deinit();

            // Build JSON response
            var json_buf = std.ArrayList(u8){};
            defer json_buf.deinit(self.allocator);

            try json_buf.appendSlice(self.allocator, "{\"series_id\":");
            var num_buf: [32]u8 = undefined;
            var num_str = std.fmt.bufPrint(&num_buf, "{d}", .{series_id}) catch "0";
            try json_buf.appendSlice(self.allocator, num_str);
            try json_buf.appendSlice(self.allocator, ",\"count\":");
            num_str = std.fmt.bufPrint(&num_buf, "{d}", .{result.points.len}) catch "0";
            try json_buf.appendSlice(self.allocator, num_str);
            try json_buf.appendSlice(self.allocator, ",\"points\":[");

            for (result.points, 0..) |point, i| {
                if (i > 0) try json_buf.append(self.allocator, ',');
                var point_buf: [128]u8 = undefined;
                const point_json = std.fmt.bufPrint(&point_buf, "{{\"ts\":{d},\"v\":{d:.6}}}", .{ point.timestamp, point.value }) catch continue;
                try json_buf.appendSlice(self.allocator, point_json);
            }

            try json_buf.appendSlice(self.allocator, "]}");

            try sendResponse(client, "200 OK", "application/json", json_buf.items);
        } else {
            try sendResponse(client, "200 OK", "application/json", "{\"series_id\":0,\"count\":0,\"points\":[]}");
        }
    }

    fn handleStatic(self: *HttpServer, client: posix.socket_t, path: []const u8) !void {
        // Normalize path
        var file_path = if (std.mem.eql(u8, path, "/")) "/index.html" else path;

        // Trim query string and carriage return
        if (std.mem.indexOfScalar(u8, file_path, '?')) |idx| {
            file_path = file_path[0..idx];
        }
        if (std.mem.indexOfScalar(u8, file_path, '\r')) |idx| {
            file_path = file_path[0..idx];
        }

        // Security: prevent directory traversal
        if (std.mem.indexOf(u8, file_path, "..") != null) {
            try sendResponse(client, "403 Forbidden", "text/plain", "Forbidden");
            return;
        }

        // Build full path
        var full_path_buf: [512]u8 = undefined;
        const full_path = std.fmt.bufPrint(&full_path_buf, "{s}{s}", .{ self.web_root, file_path }) catch {
            try sendResponse(client, "500 Internal Server Error", "text/plain", "Path too long");
            return;
        };

        // Read file
        const file = std.fs.cwd().openFile(full_path, .{}) catch {
            try sendResponse(client, "404 Not Found", "text/plain", "Not Found");
            return;
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch {
            try sendResponse(client, "500 Internal Server Error", "text/plain", "Failed to read file");
            return;
        };
        defer self.allocator.free(content);

        // Determine content type
        const content_type = getContentType(file_path);

        try sendResponse(client, "200 OK", content_type, content);
    }

    fn getContentType(path: []const u8) []const u8 {
        if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
        if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
        if (std.mem.endsWith(u8, path, ".js")) return "application/javascript; charset=utf-8";
        if (std.mem.endsWith(u8, path, ".json")) return "application/json";
        if (std.mem.endsWith(u8, path, ".png")) return "image/png";
        if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
        if (std.mem.endsWith(u8, path, ".wasm")) return "application/wasm";
        return "text/plain";
    }

    fn sendResponse(client: posix.socket_t, status: []const u8, content_type: []const u8, body: []const u8) !void {
        var header_buf: [512]u8 = undefined;
        const header = try std.fmt.bufPrint(&header_buf,
            "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
            .{ status, content_type, body.len },
        );

        _ = try posix.write(client, header);
        _ = try posix.write(client, body);
    }
};

test "http server basic" {
    const allocator = std.testing.allocator;
    var server = try HttpServer.init(allocator, 0, "src/web");
    defer server.deinit();

    try std.testing.expect(server.port > 0);
}
