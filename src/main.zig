const std = @import("std");
const cli = @import("cli");

var config = struct {
    host: []const u8 = "localhost",
    port: u16 = undefined,
}{};

pub fn main() !void {
    var r = try cli.AppRunner.init(std.heap.page_allocator);

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

var shutdown_fd: ?std.posix.fd_t = null;

fn run_server() !void {
    std.log.info("server is listening on {s}:{d}", .{ config.host, config.port });

    var sigset = std.posix.sigemptyset();
    std.posix.sigaddset(&sigset, std.posix.SIG.INT);
    std.posix.sigaddset(&sigset, std.posix.SIG.TERM);
    std.posix.sigprocmask(std.posix.SIG.BLOCK, &sigset, null);

    const sigfd = try std.posix.signalfd(-1, &sigset, 0);
    defer std.posix.close(sigfd);

    var fds = [_]std.posix.pollfd{
        .{ .fd = sigfd, .events = std.posix.POLL.IN, .revents = 0 },
        .{ .fd = shutdown_fd orelse -1, .events = std.posix.POLL.IN, .revents = 0 },
    };
    const nfds: usize = if (shutdown_fd != null) 2 else 1;

    _ = try std.posix.poll(fds[0..nfds], -1);

    if (fds[0].revents & std.posix.POLL.IN != 0) {
        var info: std.os.linux.signalfd_siginfo = undefined;
        _ = try std.posix.read(sigfd, std.mem.asBytes(&info));
        std.log.info("received signal {d}, shutting down", .{info.signo});
    } else {
        std.log.info("shutdown requested, shutting down", .{});
    }
}

test "run_server" {
    config.host = "localhost";
    config.port = 8080;

    const pipe = try std.posix.pipe();
    shutdown_fd = pipe[0];
    defer {
        std.posix.close(pipe[0]);
        std.posix.close(pipe[1]);
        shutdown_fd = null;
    }

    const thread = try std.Thread.spawn(.{}, struct {
        fn run() void {
            run_server() catch |err| {
                std.log.err("run_server failed: {}", .{err});
            };
        }
    }.run, .{});

    std.Thread.sleep(10 * std.time.ns_per_ms);
    _ = try std.posix.write(pipe[1], "x");

    thread.join();
}
