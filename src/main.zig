const std = @import("std");
const cli = @import("cli");
const testing = std.testing;
const server = @import("server/server.zig");
const http = @import("web/http.zig");
const client_mod = @import("client/client.zig");
const concurrent_tsdb_mod = @import("timeserie/concurrent_tsdb.zig");
const ConcurrentTSDB = concurrent_tsdb_mod.ConcurrentTSDB;

// Global configuration populated by CLI argument parsing
var config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = undefined,
    web_port: ?u16 = null,
}{};

// Generate command config
var gen_config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 9876,
    series_id: u64 = 1,
    count: u32 = 100,
    interval_ms: u64 = 1000,
}{};

// Entry point: sets up CLI parsing and launches the server
pub fn main() !void {
    var r = try cli.AppRunner.init(std.heap.page_allocator);

    // Define CLI command structure with subcommands
    const app = cli.App{
        .command = cli.Command{
            .name = "tsvdb",
            .target = cli.CommandTarget{
                .subcommands = try r.allocCommands(&.{
                    .{
                        .name = "serve",
                        .options = try r.allocOptions(&.{
                            .{
                                .long_name = "host",
                                .help = "host to listen on",
                                .value_ref = r.mkRef(&config.host),
                            },
                            .{
                                .long_name = "port",
                                .short_alias = 'p',
                                .help = "port to bind to",
                                .required = true,
                                .value_ref = r.mkRef(&config.port),
                            },
                            .{
                                .long_name = "web-port",
                                .short_alias = 'w',
                                .help = "port for web UI (optional)",
                                .value_ref = r.mkRef(&config.web_port),
                            },
                        }),
                        .target = cli.CommandTarget{
                            .action = cli.CommandAction{ .exec = run_server },
                        },
                    },
                    .{
                        .name = "generate",
                        .options = try r.allocOptions(&.{
                            .{
                                .long_name = "host",
                                .help = "server host",
                                .value_ref = r.mkRef(&gen_config.host),
                            },
                            .{
                                .long_name = "port",
                                .short_alias = 'p',
                                .help = "server port",
                                .value_ref = r.mkRef(&gen_config.port),
                            },
                            .{
                                .long_name = "series",
                                .short_alias = 's',
                                .help = "series ID",
                                .value_ref = r.mkRef(&gen_config.series_id),
                            },
                            .{
                                .long_name = "count",
                                .short_alias = 'n',
                                .help = "number of points to generate",
                                .value_ref = r.mkRef(&gen_config.count),
                            },
                            .{
                                .long_name = "interval",
                                .short_alias = 'i',
                                .help = "interval between points in ms",
                                .value_ref = r.mkRef(&gen_config.interval_ms),
                            },
                        }),
                        .target = cli.CommandTarget{
                            .action = cli.CommandAction{ .exec = run_generate },
                        },
                    },
                }),
            },
        },
    };
    return r.run(&app);
}

// Global state for stats and query callbacks
var global_server: ?*server.Server = null;
var global_tsdb: ?*ConcurrentTSDB = null;
var start_time: i64 = 0;

// TCP client config for HTTP server to use
var tcp_host: []const u8 = "127.0.0.1";
var tcp_port: u16 = 9876;

fn getStats() http.HttpServer.Stats {
    const allocator = std.heap.page_allocator;

    // Use TCP client to query stats from server
    var client = client_mod.Client.init(allocator, .{
        .host = tcp_host,
        .port = tcp_port,
    }) catch return .{};
    defer client.deinit();

    client.connect() catch return .{};

    const stats = client.getGlobalStats() catch return .{};

    return .{
        .series_count = stats.series_count,
        .total_points = stats.total_points,
        .writes_per_sec = 0,
        .wal_size_bytes = 0,
        .uptime_seconds = stats.uptime_seconds,
        .connected_clients = stats.connected_clients,
        .address = std.fmt.allocPrint(allocator, "{s}:{d}", .{ tcp_host, tcp_port }) catch "unknown",
    };
}

// Generate test data using client
fn run_generate() !void {
    const allocator = std.heap.page_allocator;

    std.log.info("connecting to {s}:{d}...", .{ gen_config.host, gen_config.port });

    var client = try client_mod.Client.init(allocator, .{
        .host = gen_config.host,
        .port = gen_config.port,
    });
    defer client.deinit();

    try client.connect();
    std.log.info("connected, generating {d} points for series {d}", .{ gen_config.count, gen_config.series_id });

    const gen_start_time = std.time.nanoTimestamp();
    var base_ts: i64 = std.time.milliTimestamp() * 1_000_000; // nanoseconds

    var i: u32 = 0;
    while (i < gen_config.count) : (i += 1) {
        // Generate a value with some variation (sine wave + noise)
        const t: f64 = @floatFromInt(i);
        const value = 50.0 + 30.0 * @sin(t / 10.0) + (@as(f64, @floatFromInt(i % 7)) - 3.0);

        try client.insert(gen_config.series_id, base_ts, value);
        base_ts += @as(i64, @intCast(gen_config.interval_ms)) * 1_000_000;

        // Progress every 100 points
        if ((i + 1) % 100 == 0) {
            std.log.info("inserted {d}/{d} points", .{ i + 1, gen_config.count });
        }
    }

    const elapsed_ns = std.time.nanoTimestamp() - gen_start_time;
    const elapsed_ms: f64 = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const rate: f64 = @as(f64, @floatFromInt(gen_config.count)) / (elapsed_ms / 1000.0);

    std.log.info("done! inserted {d} points in {d:.1}ms ({d:.0} points/sec)", .{ gen_config.count, elapsed_ms, rate });
}

fn queryData(allocator: std.mem.Allocator, series_id: u64, start_ts: i64, end_ts: i64) http.HttpServer.QueryResult {
    // Use TCP client to query the TSDB server
    var client = client_mod.Client.init(allocator, .{
        .host = tcp_host,
        .port = tcp_port,
    }) catch return .{ .points = &.{}, .allocator = allocator };
    defer client.deinit();

    client.connect() catch return .{ .points = &.{}, .allocator = allocator };

    const tcp_result = client.query(series_id, start_ts, end_ts) catch {
        return .{ .points = &.{}, .allocator = allocator };
    };
    defer allocator.free(tcp_result);

    // Convert to HTTP DataPoint format
    var result = std.ArrayList(http.DataPoint){};
    for (tcp_result) |p| {
        result.append(allocator, .{
            .series_id = p.series_id,
            .timestamp = p.timestamp,
            .value = p.value,
        }) catch continue;
    }

    return .{ .points = result.toOwnedSlice(allocator) catch &.{}, .allocator = allocator };
}

// Starts the server and waits for shutdown signal
fn run_server() !void {
    const allocator = std.heap.page_allocator;
    start_time = std.time.timestamp();

    // Initialize ConcurrentTSDB
    var tsdb = try ConcurrentTSDB.init(allocator, .{
        .wal_path = "tmp/data.wal",
    });
    defer tsdb.deinit();
    global_tsdb = &tsdb;
    defer {
        global_tsdb = null;
    }

    // Start concurrent consumers
    try tsdb.start();

    // Initialize the server with TSDB
    var srv = try server.Server.init(allocator, .{
        .host = config.host,
        .port = config.port,
        .tsdb = &tsdb,
    });
    defer srv.deinit();
    global_server = &srv;
    defer {
        global_server = null;
    }

    // Start listening
    try srv.start();

    // Start HTTP server for web UI if requested
    var http_server: ?http.HttpServer = null;
    if (config.web_port) |web_port| {
        http_server = try http.HttpServer.init(allocator, web_port, "zig-out/web");
        http_server.?.setStatsCallback(&getStats);
        http_server.?.setQueryCallback(&queryData);
        try http_server.?.start();
        std.log.info("web UI available at http://127.0.0.1:{d}/", .{http_server.?.port});
    }
    defer if (http_server) |*h| h.deinit();

    // Run server accept loop in a separate thread
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *server.Server) void {
            const sock = s.listener orelse return;
            var fds = [_]std.posix.pollfd{
                .{ .fd = sock, .events = std.posix.POLL.IN, .revents = 0 },
            };

            while (s.running.load(.acquire)) {
                // Poll with timeout so we can check running flag periodically
                const ret = std.posix.poll(&fds, 100) catch continue;
                if (ret == 0) continue; // timeout

                s.acceptOne() catch |err| {
                    if (!s.running.load(.acquire)) return;
                    std.log.err("accept error: {}", .{err});
                };
            }
        }
    }.run, .{&srv});

    // Block SIGINT and SIGTERM so we can handle them via signalfd
    var sigset = std.posix.sigemptyset();
    std.posix.sigaddset(&sigset, std.posix.SIG.INT);
    std.posix.sigaddset(&sigset, std.posix.SIG.TERM);
    std.posix.sigprocmask(std.posix.SIG.BLOCK, &sigset, null);

    // Create signalfd to receive signals as readable events
    const sigfd = try std.posix.signalfd(-1, &sigset, 0);
    defer std.posix.close(sigfd);

    // Poll for either a signal or a shutdown request
    var fds = [_]std.posix.pollfd{
        .{ .fd = sigfd, .events = std.posix.POLL.IN, .revents = 0 },
        .{ .fd = shutdown_fd orelse -1, .events = std.posix.POLL.IN, .revents = 0 },
    };
    const nfds: usize = if (shutdown_fd != null) 2 else 1;

    // Block until shutdown signal received
    _ = try std.posix.poll(fds[0..nfds], -1);

    // Handle signal-based shutdown (SIGINT/SIGTERM)
    if (fds[0].revents & std.posix.POLL.IN != 0) {
        var info: std.os.linux.signalfd_siginfo = undefined;
        _ = try std.posix.read(sigfd, std.mem.asBytes(&info));
        std.log.info("received signal {d}, shutting down", .{info.signo});
    } else {
        // Handle programmatic shutdown via shutdown_fd
        std.log.info("shutdown requested, shutting down", .{});
    }

    // Stop server and wait for thread to finish
    srv.stop();
    server_thread.join();
}

// Test graceful shutdown using a pipe to simulate shutdown signal

// Optional file descriptor for programmatic shutdown (used in tests)
var shutdown_fd: ?std.posix.fd_t = null;

test "run_server" {
    config.host = "127.0.0.1";
    config.port = 18080;

    // Create a pipe for triggering shutdown from the test
    const pipe = try std.posix.pipe();
    shutdown_fd = pipe[0];
    defer {
        std.posix.close(pipe[0]);
        std.posix.close(pipe[1]);
        shutdown_fd = null;
    }

    // Run server in a separate thread
    const thread = try std.Thread.spawn(.{}, struct {
        fn run() void {
            run_server() catch |err| {
                std.debug.panic("unexpected error: {}", .{err});
            };
        }
    }.run, .{});

    // Give the server time to start, then trigger shutdown
    std.Thread.sleep(10 * std.time.ns_per_ms);
    _ = try std.posix.write(pipe[1], "x");

    thread.join();
}
