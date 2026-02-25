const std = @import("std");

// Supported container runtimes
pub const ContainerRuntime = enum {
    docker,
    containerd,
    crio,
    podman,
    lxc,
    unknown,

    pub fn toString(self: ContainerRuntime) []const u8 {
        return switch (self) {
            .docker => "docker",
            .containerd => "containerd",
            .crio => "cri-o",
            .podman => "podman",
            .lxc => "lxc",
            .unknown => "unknown",
        };
    }
};

// Pattern matching info for container ID extraction
const RuntimePattern = struct {
    runtime: ContainerRuntime,
    prefix: []const u8,
    suffix: []const u8,
    id_length: usize, // Expected ID length (12 for short, 64 for full)
};

// Known runtime patterns
const runtime_patterns = [_]RuntimePattern{
    // Docker: docker-<64-char-id>.scope
    .{ .runtime = .docker, .prefix = "docker-", .suffix = ".scope", .id_length = 64 },
    // Containerd: cri-containerd-<64-char-id>.scope
    .{ .runtime = .containerd, .prefix = "cri-containerd-", .suffix = ".scope", .id_length = 64 },
    // CRI-O: crio-<64-char-id>.scope
    .{ .runtime = .crio, .prefix = "crio-", .suffix = ".scope", .id_length = 64 },
    // Podman: libpod-<64-char-id>.scope
    .{ .runtime = .podman, .prefix = "libpod-", .suffix = ".scope", .id_length = 64 },
    // Alternative containerd format
    .{ .runtime = .containerd, .prefix = "containerd-", .suffix = ".scope", .id_length = 64 },
    // LXC containers
    .{ .runtime = .lxc, .prefix = "lxc/", .suffix = "", .id_length = 0 },
    .{ .runtime = .lxc, .prefix = "lxc-", .suffix = "", .id_length = 0 },
};

// Short container ID (12 characters)
pub const ShortId = [12]u8;

// Full container ID (64 characters)
pub const FullId = [64]u8;

// Container information
pub const Container = struct {
    // Short container ID (first 12 characters)
    short_id: ShortId,
    // Full container ID (64 characters if available)
    full_id: ?FullId,
    // Container runtime
    runtime: ContainerRuntime,
    // Cgroup ID (inode number from stat)
    cgroup_id: u64,
    // Full cgroup path
    cgroup_path: []const u8,
    // Human-readable name (if available)
    name: ?[]const u8,
    // Allocator used for dynamic fields
    allocator: std.mem.Allocator,

    const Self = @This();

    // Create a container from detected cgroup
    pub fn init(
        allocator: std.mem.Allocator,
        cgroup_path: []const u8,
        cgroup_id: u64,
        runtime: ContainerRuntime,
        full_id: []const u8,
    ) !Self {
        const path_copy = try allocator.dupe(u8, cgroup_path);
        errdefer allocator.free(path_copy);

        var short_id: ShortId = undefined;
        var full_id_arr: ?FullId = null;

        if (full_id.len >= 12) {
            @memcpy(&short_id, full_id[0..12]);
        } else {
            @memset(&short_id, '0');
            @memcpy(short_id[0..full_id.len], full_id);
        }

        if (full_id.len == 64) {
            full_id_arr = undefined;
            @memcpy(&full_id_arr.?, full_id[0..64]);
        }

        return .{
            .short_id = short_id,
            .full_id = full_id_arr,
            .runtime = runtime,
            .cgroup_id = cgroup_id,
            .cgroup_path = path_copy,
            .name = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.cgroup_path);
        if (self.name) |name| {
            self.allocator.free(name);
        }
    }

    // Get short ID as string slice
    pub fn shortIdStr(self: *const Self) []const u8 {
        return &self.short_id;
    }

    // Get full ID as string slice (if available)
    pub fn fullIdStr(self: *const Self) ?[]const u8 {
        if (self.full_id) |*id| {
            return id;
        }
        return null;
    }

    // Get display name (name if available, otherwise short ID)
    pub fn displayName(self: *const Self) []const u8 {
        return self.name orelse self.shortIdStr();
    }
};

// Result of parsing a container path
pub const ParseResult = struct {
    runtime: ContainerRuntime,
    container_id: []const u8,
};

// Parse a cgroup path component to extract container info
// Returns runtime and container ID if matched
pub fn parseContainerPath(path_component: []const u8) ?ParseResult {
    for (runtime_patterns) |pattern| {
        if (matchPattern(path_component, pattern)) |id| {
            return .{
                .runtime = pattern.runtime,
                .container_id = id,
            };
        }
    }
    return null;
}

// Try to match a path component against a pattern
fn matchPattern(component: []const u8, pattern: RuntimePattern) ?[]const u8 {
    // Check prefix
    if (!std.mem.startsWith(u8, component, pattern.prefix)) {
        return null;
    }

    var remaining = component[pattern.prefix.len..];

    // Check suffix
    if (pattern.suffix.len > 0) {
        if (!std.mem.endsWith(u8, remaining, pattern.suffix)) {
            return null;
        }
        remaining = remaining[0 .. remaining.len - pattern.suffix.len];
    }

    // Validate ID length
    if (pattern.id_length > 0) {
        if (remaining.len != pattern.id_length) {
            return null;
        }
        // Validate hex characters
        for (remaining) |c| {
            if (!std.ascii.isHex(c)) {
                return null;
            }
        }
    }

    return remaining;
}

// Extract container info from a full cgroup path
pub fn parseFullPath(path: []const u8) ?ParseResult {
    // Split path and check each component
    var it = std.mem.splitScalar(u8, path, '/');
    while (it.next()) |component| {
        if (component.len == 0) continue;
        if (parseContainerPath(component)) |result| {
            return result;
        }
    }
    return null;
}

// Check if a cgroup path looks like a container
pub fn isContainerPath(path: []const u8) bool {
    return parseFullPath(path) != null;
}

// Get cgroup ID (inode number) from a path
pub fn getCgroupId(path: []const u8) !u64 {
    var stat_buf: std.posix.Stat = undefined;
    const result = std.posix.system.stat(path.ptr, &stat_buf);
    if (result != 0) {
        return error.StatFailed;
    }
    return stat_buf.ino;
}

// Get cgroup ID using stat syscall with proper path handling
pub fn getCgroupIdSafe(path: []const u8, allocator: std.mem.Allocator) !u64 {
    // Ensure null-terminated path
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var stat_buf: std.posix.Stat = undefined;
    const result = std.posix.system.stat(path_z.ptr, &stat_buf);
    if (result != 0) {
        return error.StatFailed;
    }
    return stat_buf.ino;
}

test "parse docker container path" {
    const result = parseContainerPath("docker-a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2.scope");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(ContainerRuntime.docker, result.?.runtime);
    try std.testing.expectEqual(@as(usize, 64), result.?.container_id.len);
}

test "parse containerd container path" {
    const result = parseContainerPath("cri-containerd-abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.scope");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(ContainerRuntime.containerd, result.?.runtime);
}

test "parse crio container path" {
    const result = parseContainerPath("crio-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.scope");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(ContainerRuntime.crio, result.?.runtime);
}

test "parse podman container path" {
    const result = parseContainerPath("libpod-fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210.scope");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(ContainerRuntime.podman, result.?.runtime);
}

test "parse full path" {
    const result = parseFullPath("/sys/fs/cgroup/system.slice/docker-a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2.scope");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(ContainerRuntime.docker, result.?.runtime);
}

test "invalid path rejected" {
    const result = parseContainerPath("init.scope");
    try std.testing.expect(result == null);
}

test "short id rejected" {
    const result = parseContainerPath("docker-a1b2c3d4e5f6.scope");
    try std.testing.expect(result == null);
}
