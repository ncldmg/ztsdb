const std = @import("std");

fn buildModule(b: *std.Build, target: std.Build.ResolvedTarget) *std.Build.Module {
    const mod = b.addModule("tsvdb", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });
    return mod;
}

fn buildModuleTest(b: *std.Build, mod: *std.Build.Module) *std.Build.Step.Compile {
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    return mod_tests;
}

fn buildExecutable(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mod: *std.Build.Module,
) *std.Build.Step.Compile {
    const cli = b.dependency("cli", .{
        .target = target,
        .optimize = optimize,
    });
    const exe = b.addExecutable(.{
        .name = "tsvdb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "tsvdb", .module = mod },
                .{ .name = "cli", .module = cli.module("cli") },
            },
        }),
    });
    return exe;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = buildModule(b, target);
    const mod_tests = buildModuleTest(b, mod);
    const exe = buildExecutable(b, target, optimize, mod);

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // Build test binary without running
    const lib_test_step = b.step("lib-test", "Build library test binary");
    b.installArtifact(mod_tests);
    lib_test_step.dependOn(&b.addInstallArtifact(mod_tests, .{}).step);

    // Copy web assets to zig-out/web
    const install_html = b.addInstallFile(b.path("src/web/index.html"), "web/index.html");

    const web_step = b.step("web", "Copy web UI assets");
    web_step.dependOn(&install_html.step);

    // Benchmark executable
    const bench_exe = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast, // Always optimize benchmarks
        }),
    });
    b.installArtifact(bench_exe);

    const bench_run = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&bench_run.step);
}
