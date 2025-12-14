const std = @import("std");
const cli = @import("cli");
const testing = std.testing;
const server = @import("server/server.zig");

// Global configuration populated by CLI argument parsing
var config = struct {
    host: []const u8 = "localhost",
    port: u16 = undefined,
}{};

// Entry point: sets up CLI parsing and launches the server
pub fn main() !void {
    var r = try cli.AppRunner.init(std.heap.page_allocator);

    // Define CLI command structure with host/port options
    const app = cli.App{
        .command = cli.Command{
            .name = "short",
            .options = try r.allocOptions(&.{
                .{
                    .long_name = "host",
                    .help = "host to listen on",
                    .value_ref = r.mkRef(&config.host),
                },

                .{
                    .long_name = "port",
                    .help = "port to bind to",
                    .required = true,
                    .value_ref = r.mkRef(&config.port),
                },
            }),
            .target = cli.CommandTarget{
                .action = cli.CommandAction{ .exec = run_server },
            },
        },
    };
    return r.run(&app);
}

// Starts the server and waits for shutdown signal
fn run_server() !void {
    const allocator = std.heap.page_allocator;

    // Initialize the server
    var srv = try server.Server.init(allocator, .{
        .host = config.host,
        .port = config.port,
    });
    defer srv.deinit();

    // Start listening
    try srv.start();
    std.log.info("server is listening on {s}:{d}", .{ config.host, config.port });

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
