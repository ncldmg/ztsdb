const std = @import("std");
const container_mod = @import("container.zig");

const Container = container_mod.Container;
const ContainerRuntime = container_mod.ContainerRuntime;
const Allocator = std.mem.Allocator;

pub const DetectorError = error{
    CgroupRootNotFound,
    PermissionDenied,
    WalkFailed,
    OutOfMemory,
    StatFailed,
};

// Container discovery result
pub const DiscoveryResult = struct {
    allocator: Allocator,
    // Newly discovered containers
    new_containers: std.ArrayListUnmanaged(Container),
    // Removed container cgroup IDs
    removed_cgroup_ids: std.ArrayListUnmanaged(u64),

    pub fn deinit(self: *DiscoveryResult) void {
        for (self.new_containers.items) |*c| {
            c.deinit();
        }
        self.new_containers.deinit(self.allocator);
        self.removed_cgroup_ids.deinit(self.allocator);
    }
};

// Cgroup hierarchy detector
pub const Detector = struct {
    allocator: Allocator,
    // Cgroup v2 root path
    cgroup_root: []const u8,
    // Known containers by cgroup ID
    containers: std.AutoHashMap(u64, Container),
    // Last scan time
    last_scan_ns: i128,

    const Self = @This();

    pub fn init(allocator: Allocator, cgroup_root: ?[]const u8) !Self {
        const root = cgroup_root orelse "/sys/fs/cgroup";

        // Verify cgroup root exists
        std.fs.accessAbsolute(root, .{}) catch {
            return DetectorError.CgroupRootNotFound;
        };

        const root_copy = try allocator.dupe(u8, root);

        return .{
            .allocator = allocator,
            .cgroup_root = root_copy,
            .containers = std.AutoHashMap(u64, Container).init(allocator),
            .last_scan_ns = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.containers.valueIterator();
        while (it.next()) |container| {
            container.deinit();
        }
        self.containers.deinit();
        self.allocator.free(self.cgroup_root);
    }

    // Scan for containers and return changes since last scan
    pub fn scan(self: *Self) !DiscoveryResult {
        var result = DiscoveryResult{
            .allocator = self.allocator,
            .new_containers = .{},
            .removed_cgroup_ids = .{},
        };
        errdefer result.deinit();

        // Track which cgroups we've seen in this scan
        var seen = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen.deinit();

        // Walk the cgroup hierarchy
        try self.walkDirectory(self.cgroup_root, &result, &seen);

        // Find removed containers
        var to_remove = std.ArrayListUnmanaged(u64){};
        defer to_remove.deinit(self.allocator);

        var it = self.containers.keyIterator();
        while (it.next()) |cgroup_id| {
            if (!seen.contains(cgroup_id.*)) {
                try to_remove.append(self.allocator, cgroup_id.*);
                try result.removed_cgroup_ids.append(self.allocator, cgroup_id.*);
            }
        }

        // Remove stale containers
        for (to_remove.items) |cgroup_id| {
            if (self.containers.fetchRemove(cgroup_id)) |entry| {
                var container = entry.value;
                container.deinit();
            }
        }

        self.last_scan_ns = std.time.nanoTimestamp();

        return result;
    }

    // Walk a directory recursively looking for containers
    fn walkDirectory(
        self: *Self,
        path: []const u8,
        result: *DiscoveryResult,
        seen: *std.AutoHashMap(u64, void),
    ) !void {
        var dir = std.fs.openDirAbsolute(path, .{ .iterate = true }) catch |err| {
            // Skip directories we can't access
            if (err == error.AccessDenied or err == error.FileNotFound) {
                return;
            }
            return err;
        };
        defer dir.close();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .directory) continue;

            // Skip hidden and system directories
            if (entry.name[0] == '.') continue;
            if (std.mem.eql(u8, entry.name, "cpu") or
                std.mem.eql(u8, entry.name, "cpuacct") or
                std.mem.eql(u8, entry.name, "memory") or
                std.mem.eql(u8, entry.name, "devices") or
                std.mem.eql(u8, entry.name, "freezer") or
                std.mem.eql(u8, entry.name, "net_cls") or
                std.mem.eql(u8, entry.name, "blkio") or
                std.mem.eql(u8, entry.name, "perf_event") or
                std.mem.eql(u8, entry.name, "hugetlb") or
                std.mem.eql(u8, entry.name, "pids") or
                std.mem.eql(u8, entry.name, "rdma") or
                std.mem.eql(u8, entry.name, "misc"))
            {
                continue;
            }

            // Build full path
            const full_path = try std.fs.path.join(self.allocator, &.{ path, entry.name });
            defer self.allocator.free(full_path);

            // Check if this is a container
            if (container_mod.parseContainerPath(entry.name)) |info| {
                try self.handleContainer(full_path, info.runtime, info.container_id, result, seen);
            }

            // Recurse into subdirectories
            try self.walkDirectory(full_path, result, seen);
        }
    }

    // Handle a discovered container
    fn handleContainer(
        self: *Self,
        cgroup_path: []const u8,
        runtime: ContainerRuntime,
        container_id: []const u8,
        result: *DiscoveryResult,
        seen: *std.AutoHashMap(u64, void),
    ) !void {
        // Get cgroup ID
        const cgroup_id = container_mod.getCgroupIdSafe(cgroup_path, self.allocator) catch {
            return; // Skip if we can't stat
        };

        try seen.put(cgroup_id, {});

        // Check if we already know this container
        if (self.containers.contains(cgroup_id)) {
            return;
        }

        // Create new container
        var container = try Container.init(
            self.allocator,
            cgroup_path,
            cgroup_id,
            runtime,
            container_id,
        );
        errdefer container.deinit();

        // Add to known containers
        try self.containers.put(cgroup_id, container);

        // Copy for result (transfers ownership)
        const container_copy = try Container.init(
            self.allocator,
            cgroup_path,
            cgroup_id,
            runtime,
            container_id,
        );
        try result.new_containers.append(self.allocator, container_copy);
    }

    // Get all known containers
    pub fn getContainers(self: *Self) []const Container {
        // Return as slice - note this is not ideal but works for iteration
        _ = self;
        @compileError("Use containerIterator() instead");
    }

    // Iterate over known containers
    pub fn containerIterator(self: *Self) std.AutoHashMap(u64, Container).ValueIterator {
        return self.containers.valueIterator();
    }

    // Get container by cgroup ID
    pub fn getContainer(self: *Self, cgroup_id: u64) ?*const Container {
        return self.containers.getPtr(cgroup_id);
    }

    // Get container count
    pub fn containerCount(self: *const Self) usize {
        return self.containers.count();
    }

    // Force rescan of a specific path
    pub fn rescanPath(self: *Self, path: []const u8) !?Container {
        if (container_mod.parseFullPath(path)) |info| {
            const cgroup_id = try container_mod.getCgroupIdSafe(path, self.allocator);

            if (self.containers.contains(cgroup_id)) {
                return self.containers.get(cgroup_id);
            }

            var container = try Container.init(
                self.allocator,
                path,
                cgroup_id,
                info.runtime,
                info.container_id,
            );
            errdefer container.deinit();

            try self.containers.put(cgroup_id, container);
            return container;
        }
        return null;
    }
};

// Quick check if cgroup v2 is available
pub fn isCgroupV2Available() bool {
    // Check if /sys/fs/cgroup/cgroup.controllers exists (v2 indicator)
    std.fs.accessAbsolute("/sys/fs/cgroup/cgroup.controllers", .{}) catch {
        return false;
    };
    return true;
}

// Get the cgroup v2 root path
pub fn getCgroupRoot() []const u8 {
    return "/sys/fs/cgroup";
}

test "detector init" {
    // This test would fail on systems without cgroup v2
    const allocator = std.testing.allocator;

    var detector = Detector.init(allocator, null) catch {
        // Expected on systems without cgroup v2
        return;
    };
    defer detector.deinit();
    try std.testing.expect(detector.containerCount() == 0);
}
