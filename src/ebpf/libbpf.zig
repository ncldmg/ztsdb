//! libbpf bindings for BPF program loading and management.
//!
//! This module wraps libbpf C library functions for:
//! - Loading BPF objects from ELF files or embedded bytes
//! - Creating and managing BPF maps
//! - Attaching programs to hooks (tracepoints, cgroups, etc.)
//!
//! Build requirements:
//!   Link with: -lbpf -lelf -lz
//!   Headers: bpf/libbpf.h, bpf/bpf.h

const std = @import("std");
const Allocator = std.mem.Allocator;

// Import libbpf C headers
const c = @cImport({
    @cInclude("bpf/libbpf.h");
    @cInclude("bpf/bpf.h");
    @cInclude("errno.h");
});

pub const Error = error{
    OpenFailed,
    LoadFailed,
    AttachFailed,
    MapNotFound,
    ProgramNotFound,
    LookupFailed,
    UpdateFailed,
    DeleteFailed,
    PermissionDenied,
    OutOfMemory,
    InvalidArgument,
};

/// BPF object handle - wraps libbpf's bpf_object
pub const Object = struct {
    obj: *c.bpf_object,

    const Self = @This();

    /// Open a BPF object from a file path
    pub fn openFile(path: [:0]const u8) Error!Self {
        const obj = c.bpf_object__open(path.ptr);
        if (obj == null) {
            return Error.OpenFailed;
        }
        return Self{ .obj = obj.? };
    }

    /// Open a BPF object from memory (for embedded objects)
    pub fn openMemory(data: []const u8, name: [:0]const u8) Error!Self {
        _ = name; // Name is set via BTF in the object
        const obj = c.bpf_object__open_mem(data.ptr, data.len, null);
        if (obj == null) {
            return Error.OpenFailed;
        }
        return Self{ .obj = obj.? };
    }

    /// Load the BPF object into the kernel
    pub fn load(self: Self) Error!void {
        const ret = c.bpf_object__load(self.obj);
        if (ret != 0) {
            return Error.LoadFailed;
        }
    }

    /// Find a map by name
    pub fn findMap(self: Self, name: [:0]const u8) ?Map {
        const map = c.bpf_object__find_map_by_name(self.obj, name.ptr);
        if (map == null) {
            return null;
        }
        return Map{ .map = map.? };
    }

    /// Find a program by name
    pub fn findProgram(self: Self, name: [:0]const u8) ?Program {
        const prog = c.bpf_object__find_program_by_name(self.obj, name.ptr);
        if (prog == null) {
            return null;
        }
        return Program{ .prog = prog.? };
    }

    /// Find a program by section name (e.g., "tp/sched/sched_switch")
    pub fn findProgramBySection(self: Self, section: []const u8) ?Program {
        var iter: ?*c.bpf_program = null;
        while (true) {
            iter = c.bpf_object__next_program(self.obj, iter);
            if (iter == null) break;

            const sec_name = c.bpf_program__section_name(iter.?);
            if (sec_name != null) {
                const sec_slice = std.mem.span(sec_name.?);
                if (std.mem.eql(u8, sec_slice, section)) {
                    return Program{ .prog = iter.? };
                }
            }
        }
        return null;
    }

    /// Close the BPF object and free resources
    pub fn close(self: Self) void {
        c.bpf_object__close(self.obj);
    }
};

/// BPF map handle
pub const Map = struct {
    map: *c.bpf_map,

    const Self = @This();

    /// Get the map file descriptor
    pub fn fd(self: Self) i32 {
        return c.bpf_map__fd(self.map);
    }

    /// Get map name
    pub fn name(self: Self) []const u8 {
        const n = c.bpf_map__name(self.map);
        if (n == null) return "";
        return std.mem.span(n.?);
    }

    /// Get map type
    pub fn mapType(self: Self) u32 {
        return c.bpf_map__type(self.map);
    }

    /// Get key size
    pub fn keySize(self: Self) u32 {
        return c.bpf_map__key_size(self.map);
    }

    /// Get value size
    pub fn valueSize(self: Self) u32 {
        return c.bpf_map__value_size(self.map);
    }

    /// Get max entries
    pub fn maxEntries(self: Self) u32 {
        return c.bpf_map__max_entries(self.map);
    }

    /// Look up an element
    pub fn lookup(self: Self, key: *const anyopaque, value: *anyopaque) Error!void {
        const ret = c.bpf_map__lookup_elem(self.map, key, self.keySize(), value, self.valueSize(), 0);
        if (ret != 0) {
            return Error.LookupFailed;
        }
    }

    /// Update an element
    pub fn update(self: Self, key: *const anyopaque, value: *const anyopaque, flags: u64) Error!void {
        const ret = c.bpf_map__update_elem(self.map, key, self.keySize(), value, self.valueSize(), flags);
        if (ret != 0) {
            return Error.UpdateFailed;
        }
    }

    /// Delete an element
    pub fn delete(self: Self, key: *const anyopaque) Error!void {
        const ret = c.bpf_map__delete_elem(self.map, key, self.keySize(), 0);
        if (ret != 0) {
            return Error.DeleteFailed;
        }
    }
};

/// BPF program handle
pub const Program = struct {
    prog: *c.bpf_program,

    const Self = @This();

    /// Get the program file descriptor
    pub fn fd(self: Self) i32 {
        return c.bpf_program__fd(self.prog);
    }

    /// Get program name
    pub fn name(self: Self) []const u8 {
        const n = c.bpf_program__name(self.prog);
        if (n == null) return "";
        return std.mem.span(n.?);
    }

    /// Get section name
    pub fn section(self: Self) []const u8 {
        const n = c.bpf_program__section_name(self.prog);
        if (n == null) return "";
        return std.mem.span(n.?);
    }

    /// Attach to a tracepoint
    pub fn attachTracepoint(self: Self, category: [:0]const u8, tp_name: [:0]const u8) Error!Link {
        const link = c.bpf_program__attach_tracepoint(self.prog, category.ptr, tp_name.ptr);
        if (link == null) {
            return Error.AttachFailed;
        }
        return Link{ .link = link.? };
    }

    /// Attach to a cgroup
    pub fn attachCgroup(self: Self, cgroup_fd: i32) Error!Link {
        const link = c.bpf_program__attach_cgroup(self.prog, cgroup_fd);
        if (link == null) {
            return Error.AttachFailed;
        }
        return Link{ .link = link.? };
    }

    /// Attach using perf event
    pub fn attachPerfEvent(self: Self, perf_fd: i32) Error!Link {
        // Use bpf_link_create for perf events
        var opts = std.mem.zeroes(c.bpf_link_create_opts);
        opts.sz = @sizeOf(c.bpf_link_create_opts);

        const link_fd = c.bpf_link_create(self.fd(), perf_fd, c.BPF_PERF_EVENT, &opts);
        if (link_fd < 0) {
            return Error.AttachFailed;
        }

        // Wrap in a Link struct (we need to track the fd)
        return Link{ .link = null, .fd_only = link_fd };
    }
};

/// BPF link handle (for attached programs)
pub const Link = struct {
    link: ?*c.bpf_link,
    fd_only: i32 = -1,

    const Self = @This();

    /// Get the link file descriptor
    pub fn fd(self: Self) i32 {
        if (self.link) |l| {
            return c.bpf_link__fd(l);
        }
        return self.fd_only;
    }

    /// Detach and destroy the link
    pub fn destroy(self: Self) void {
        if (self.link) |l| {
            _ = c.bpf_link__destroy(l);
        } else if (self.fd_only >= 0) {
            _ = std.c.close(self.fd_only);
        }
    }
};

// ============================================================================
// Low-level map operations (using file descriptors directly)
// ============================================================================

/// Look up element by fd
pub fn mapLookupElem(map_fd: i32, key: *const anyopaque, value: *anyopaque, value_size: usize) Error!void {
    const ret = c.bpf_map_lookup_elem(map_fd, key, value);
    _ = value_size;
    if (ret != 0) {
        const err = std.c._errno().*;
        return switch (err) {
            c.ENOENT => Error.LookupFailed,
            c.EPERM => Error.PermissionDenied,
            else => Error.LookupFailed,
        };
    }
}

/// Update element by fd
pub fn mapUpdateElem(map_fd: i32, key: *const anyopaque, value: *const anyopaque, flags: u64) Error!void {
    const ret = c.bpf_map_update_elem(map_fd, key, value, flags);
    if (ret != 0) {
        return Error.UpdateFailed;
    }
}

/// Delete element by fd
pub fn mapDeleteElem(map_fd: i32, key: *const anyopaque) Error!void {
    const ret = c.bpf_map_delete_elem(map_fd, key);
    if (ret != 0) {
        return Error.DeleteFailed;
    }
}

/// Get next key for iteration
pub fn mapGetNextKey(map_fd: i32, key: ?*const anyopaque, next_key: *anyopaque) Error!void {
    const ret = c.bpf_map_get_next_key(map_fd, key, next_key);
    if (ret != 0) {
        const err = std.c._errno().*;
        return switch (err) {
            c.ENOENT => Error.LookupFailed, // No more keys
            else => Error.LookupFailed,
        };
    }
}

// ============================================================================
// Cgroup operations
// ============================================================================

/// Attach program to cgroup using prog_attach (for BPF_CGROUP_INET_INGRESS, etc.)
pub fn progAttachCgroup(cgroup_fd: i32, prog_fd: i32, attach_type: u32) Error!void {
    const ret = c.bpf_prog_attach(prog_fd, cgroup_fd, @intCast(attach_type), 0);
    if (ret != 0) {
        return Error.AttachFailed;
    }
}

/// Detach program from cgroup
pub fn progDetachCgroup(cgroup_fd: i32, prog_fd: i32, attach_type: u32) Error!void {
    const ret = c.bpf_prog_detach2(prog_fd, cgroup_fd, @intCast(attach_type));
    if (ret != 0) {
        // Ignore errors on detach (program might already be detached)
    }
}

// ============================================================================
// Utility functions
// ============================================================================

/// Get number of possible CPUs (for per-CPU maps)
pub fn numPossibleCpus() !u32 {
    const n = c.libbpf_num_possible_cpus();
    if (n < 0) {
        return error.InvalidArgument;
    }
    return @intCast(n);
}


// ============================================================================
// Per-CPU map reader
// ============================================================================

/// Reader for per-CPU hash maps with automatic CPU aggregation
pub fn PerCpuMapReader(comptime V: type) type {
    return struct {
        map_fd: i32,
        num_cpus: u32,
        percpu_buffer: []V,
        allocator: Allocator,

        const Self = @This();

        pub fn init(allocator: Allocator, map_fd: i32) !Self {
            const num_cpus = try numPossibleCpus();
            const buffer = try allocator.alloc(V, num_cpus);

            return Self{
                .map_fd = map_fd,
                .num_cpus = num_cpus,
                .percpu_buffer = buffer,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.percpu_buffer);
        }

        /// Read and aggregate value across all CPUs
        pub fn readAggregated(self: *Self, key: *const anyopaque, aggregate: *const fn (*V, *const V) void) !V {
            const ret = c.bpf_map_lookup_elem(self.map_fd, key, @ptrCast(self.percpu_buffer.ptr));
            if (ret != 0) {
                return Error.LookupFailed;
            }

            var result = std.mem.zeroes(V);
            for (self.percpu_buffer[0..self.num_cpus]) |*cpu_val| {
                aggregate(&result, cpu_val);
            }

            return result;
        }

        /// Iterate over all keys
        pub fn iterate(self: *Self, comptime K: type, callback: *const fn (K, V) void, aggregate: *const fn (*V, *const V) void) !void {
            var key: K = undefined;
            var next_key: K = undefined;

            // Get first key
            if (c.bpf_map_get_next_key(self.map_fd, null, &next_key) != 0) {
                return; // Empty map
            }

            while (true) {
                key = next_key;

                if (self.readAggregated(&key, aggregate)) |value| {
                    callback(key, value);
                } else |_| {}

                if (c.bpf_map_get_next_key(self.map_fd, &key, &next_key) != 0) {
                    break;
                }
            }
        }
    };
}
