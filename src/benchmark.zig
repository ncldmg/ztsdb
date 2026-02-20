const std = @import("std");
const fs = std.fs;
const net = std.net;
const posix = std.posix;
const time = std.time;
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const Server = @import("server/server.zig").Server;
const Client = @import("client/client.zig").Client;
const BufferedClient = @import("client/client.zig").BufferedClient;
const TSDB = @import("timeserie/tsdb.zig").TSDB;
const protocol = @import("protocol/protocol.zig");
const DataPoint = protocol.DataPoint;

const print = std.debug.print;

const BENCH_DURATION_NS: i128 = 2 * std.time.ns_per_s;

fn formatNumber(value: f64) struct { val: f64, suffix: []const u8 } {
    if (value >= 1_000_000_000) {
        return .{ .val = value / 1_000_000_000, .suffix = "B" };
    } else if (value >= 1_000_000) {
        return .{ .val = value / 1_000_000, .suffix = "M" };
    } else if (value >= 1_000) {
        return .{ .val = value / 1_000, .suffix = "K" };
    } else {
        return .{ .val = value, .suffix = "" };
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    print("\n", .{});
    print("╔════════════════════════════════════════════════════════════╗\n", .{});
    print("║         ZTSDB End-to-End Benchmark Suite                   ║\n", .{});
    print("║         (Client -> Server -> Storage)                      ║\n", .{});
    print("╚════════════════════════════════════════════════════════════╝\n", .{});
    print("\n", .{});

    // Benchmark with different storage configurations
    try benchmarkStorage(allocator, .hot_only);
    try benchmarkStorage(allocator, .with_warm);
    try benchmarkStorage(allocator, .with_cold);

    print("\n", .{});
    print("Benchmarks complete.\n", .{});
}

const StorageMode = enum {
    hot_only,
    with_warm,
    with_cold,
};

fn benchmarkStorage(allocator: Allocator, mode: StorageMode) !void {
    const mode_name = switch (mode) {
        .hot_only => "HOT ONLY (In-Memory)",
        .with_warm => "HOT + WARM (Mmap)",
        .with_cold => "HOT + WARM + COLD",
    };

    print("═══ {s} ═══\n\n", .{mode_name});

    // Use different paths for each mode to avoid cleanup issues
    const wal_path = switch (mode) {
        .hot_only => "/tmp/bench_hot_wal.bin",
        .with_warm => "/tmp/bench_warm_wal.bin",
        .with_cold => "/tmp/bench_cold_wal.bin",
    };
    const data_dir: ?[]const u8 = switch (mode) {
        .hot_only => null,
        .with_warm => "/tmp/bench_warm_data",
        .with_cold => "/tmp/bench_cold_data",
    };

    // Pre-cleanup
    fs.cwd().deleteFile(wal_path) catch {};
    if (data_dir) |dir| {
        fs.cwd().deleteTree(dir) catch {};
    }

    // Post-cleanup (deferred)
    defer {
        fs.cwd().deleteFile(wal_path) catch {};
        if (data_dir) |dir| {
            fs.cwd().deleteTree(dir) catch {};
        }
    }

    var tsdb = switch (mode) {
        .hot_only => try TSDB.init(allocator, .{
            .wal_path = wal_path,
            .chunk_capacity = 10_000_000,
            .ingestion_buffer_capacity = 1_000_000,
            .num_partitions = 8,
        }),
        .with_warm => try TSDB.init(allocator, .{
            .wal_path = wal_path,
            .chunk_capacity = 500_000,
            .enable_tiered_storage = true,
            .data_dir = data_dir,
            .wal_segment_size = 256 * 1024 * 1024,
            .max_warm_chunks = 1000,
            .ingestion_buffer_capacity = 1_000_000,
            .num_partitions = 8,
        }),
        .with_cold => try TSDB.init(allocator, .{
            .wal_path = wal_path,
            .chunk_capacity = 500_000,
            .enable_tiered_storage = true,
            .data_dir = data_dir,
            .wal_segment_size = 256 * 1024 * 1024,
            .max_warm_chunks = 50,
            .ingestion_buffer_capacity = 4_000_000,
            .num_partitions = 8,
        }),
    };
    defer tsdb.deinit();

    // Start concurrent consumers
    try tsdb.start();

    // Create and start server
    var server = try Server.init(allocator, .{
        .host = "127.0.0.1",
        .port = 0, // OS assigns port
        .tsdb = &tsdb,
    });
    defer server.deinit();

    try server.start();
    const port = server.address.getPort();

    // Run server accept loop in background
    const server_thread = try Thread.spawn(.{}, runServerLoop, .{&server});

    // Give server time to start
    Thread.sleep(10 * std.time.ns_per_ms);

    // Run benchmarks
    try benchmarkSingleInsert(allocator, port, mode);
    try benchmarkBufferedInsert(allocator, port, mode);
    try benchmarkBatchInsert(allocator, port, mode);

    // Stop server
    server.stop();
    server_thread.join();

    print("\n", .{});
}

fn runServerLoop(server: *Server) void {
    const sock = server.listener orelse return;
    var fds = [_]posix.pollfd{
        .{ .fd = sock, .events = posix.POLL.IN, .revents = 0 },
    };

    while (server.running.load(.acquire)) {
        // Poll with 50ms timeout to check running flag periodically
        const ret = posix.poll(&fds, 50) catch continue;
        if (ret == 0) continue; // timeout, check running flag again

        server.acceptOne() catch |err| {
            if (!server.running.load(.acquire)) return;
            if (err != error.ConnectionResetByPeer and err != error.BrokenPipe) {
                std.log.debug("accept error: {}", .{err});
            }
        };
    }
}

fn benchmarkSingleInsert(allocator: Allocator, port: u16, mode: StorageMode) !void {
    _ = mode;

    var client = try Client.init(allocator, .{
        .host = "127.0.0.1",
        .port = port,
    });
    defer client.deinit();

    try client.connect();

    // Warmup
    for (0..1000) |i| {
        try client.insert(1, @intCast(i), @floatFromInt(i));
    }

    // Benchmark
    var count: usize = 0;
    const base_ts: i64 = time.milliTimestamp() * 1_000_000;
    const start = time.nanoTimestamp();
    const deadline = start + BENCH_DURATION_NS;

    while (time.nanoTimestamp() < deadline) {
        try client.insert(1, base_ts + @as(i64, @intCast(count)), @floatFromInt(count));
        count += 1;
    }
    const elapsed = time.nanoTimestamp() - start;

    const throughput = @as(f64, @floatFromInt(count)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
    const fmt = formatNumber(throughput);
    print("  Single insert:       {d:.2}{s} pts/sec\n", .{ fmt.val, fmt.suffix });
}

fn benchmarkBufferedInsert(allocator: Allocator, port: u16, mode: StorageMode) !void {
    _ = mode;

    var client = try BufferedClient.init(allocator, .{
        .host = "127.0.0.1",
        .port = port,
        .buffer_capacity = 1000, // Same as batch size for fair comparison
    });
    defer client.deinit();

    try client.connect();

    // Warmup
    for (0..1000) |i| {
        try client.insert(1, @intCast(i), @floatFromInt(i));
    }
    try client.flush();

    // Benchmark
    var count: usize = 0;
    const base_ts: i64 = time.milliTimestamp() * 1_000_000;
    const start = time.nanoTimestamp();
    const deadline = start + BENCH_DURATION_NS;

    while (time.nanoTimestamp() < deadline) {
        try client.insert(1, base_ts + @as(i64, @intCast(count)), @floatFromInt(count));
        count += 1;
    }
    try client.flush(); // Flush remaining
    const elapsed = time.nanoTimestamp() - start;

    const throughput = @as(f64, @floatFromInt(count)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
    const fmt = formatNumber(throughput);
    print("  Buffered insert:     {d:.2}{s} pts/sec  (buffer=1000)\n", .{ fmt.val, fmt.suffix });
}

fn benchmarkBatchInsert(allocator: Allocator, port: u16, mode: StorageMode) !void {
    _ = mode;

    var client = try Client.init(allocator, .{
        .host = "127.0.0.1",
        .port = port,
    });
    defer client.deinit();

    try client.connect();

    const batch_size: usize = 1000;
    const batch = try allocator.alloc(DataPoint, batch_size);
    defer allocator.free(batch);

    // Warmup
    for (batch, 0..) |*p, i| {
        p.* = .{ .series_id = 2, .timestamp = @intCast(i), .value = @floatFromInt(i) };
    }
    try client.insertBatch(batch);

    // Benchmark
    var count: usize = 0;
    const base_ts: i64 = time.milliTimestamp() * 1_000_000;
    const start = time.nanoTimestamp();
    const deadline = start + BENCH_DURATION_NS;

    while (time.nanoTimestamp() < deadline) {
        for (batch, 0..) |*p, i| {
            p.* = .{
                .series_id = 2,
                .timestamp = base_ts + @as(i64, @intCast(count + i)),
                .value = @floatFromInt(count + i),
            };
        }
        try client.insertBatch(batch);
        count += batch_size;
    }
    const elapsed = time.nanoTimestamp() - start;

    const throughput = @as(f64, @floatFromInt(count)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
    const fmt = formatNumber(throughput);
    print("  Batch insert:        {d:.2}{s} pts/sec  (batch={d})\n", .{ fmt.val, fmt.suffix, batch_size });
}

fn benchmarkMultiClient(allocator: Allocator, port: u16, mode: StorageMode) !void {
    _ = mode;

    const client_counts = [_]usize{ 1, 2 };

    for (client_counts) |num_clients| {
        var total_count = std.atomic.Value(usize).init(0);
        var running = std.atomic.Value(bool).init(true);

        const threads = try allocator.alloc(Thread, num_clients);
        defer allocator.free(threads);

        const base_ts: i64 = time.milliTimestamp() * 1_000_000;
        const start = time.nanoTimestamp();

        // Start client threads
        for (threads, 0..) |*t, client_id| {
            t.* = try Thread.spawn(.{}, clientWorker, .{
                allocator,
                port,
                client_id,
                base_ts,
                &running,
                &total_count,
            });
        }

        // Run for benchmark duration
        Thread.sleep(@intCast(BENCH_DURATION_NS));
        running.store(false, .release);

        // Wait for threads with timeout
        for (threads) |t| {
            t.join();
        }

        const elapsed = time.nanoTimestamp() - start;
        const count = total_count.load(.acquire);
        const throughput = @as(f64, @floatFromInt(count)) / (@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0);
        const fmt = formatNumber(throughput);

        print("  {d} client(s):   {d:.2}{s} pts/sec\n", .{ num_clients, fmt.val, fmt.suffix });
    }
}

fn clientWorker(
    allocator: Allocator,
    port: u16,
    client_id: usize,
    base_ts: i64,
    running: *std.atomic.Value(bool),
    total_count: *std.atomic.Value(usize),
) void {
    var client = Client.init(allocator, .{
        .host = "127.0.0.1",
        .port = port,
    }) catch return;
    defer client.deinit();

    client.connect() catch return;

    const batch_size: usize = 100;
    const batch = allocator.alloc(DataPoint, batch_size) catch return;
    defer allocator.free(batch);

    var local_count: usize = 0;
    const series_id: u64 = @intCast(client_id + 100);
    var consecutive_errors: usize = 0;

    while (running.load(.acquire)) {
        for (batch, 0..) |*p, i| {
            p.* = .{
                .series_id = series_id,
                .timestamp = base_ts + @as(i64, @intCast(local_count + i)),
                .value = @floatFromInt(local_count + i),
            };
        }
        client.insertBatch(batch) catch {
            consecutive_errors += 1;
            if (!running.load(.acquire) or consecutive_errors > 10) break;
            Thread.sleep(1 * time.ns_per_ms);
            continue;
        };
        consecutive_errors = 0;
        local_count += batch_size;
    }

    _ = total_count.fetchAdd(local_count, .release);
}
