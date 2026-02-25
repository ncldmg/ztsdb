const std = @import("std");
const root = @import("../root.zig");
const libbpf = @import("../ebpf/libbpf.zig");
const detector = @import("../cgroup/detector.zig");
const container = @import("../cgroup/container.zig");
const series = @import("series.zig");
const metrics_mod = @import("metrics.zig");
const registry_mod = @import("../metrics/registry.zig");

const TSDB = root.TSDB;
const Allocator = std.mem.Allocator;
const Container = container.Container;
const MetricType = series.MetricType;
const Registry = registry_mod.Registry;

// Cgroup metrics structure - must match cgroupmon.bpf.c exactly
pub const CgroupMetrics = extern struct {
    cpu_time_ns: u64,
    memory_bytes: u64,
    net_rx_bytes: u64,
    net_tx_bytes: u64,
    net_rx_packets: u64,
    net_tx_packets: u64,
    block_read_bytes: u64,
    block_write_bytes: u64,
    block_read_ops: u64,
    block_write_ops: u64,
    timestamp: u64,

    pub fn zero() CgroupMetrics {
        return std.mem.zeroes(CgroupMetrics);
    }

    // Add another metrics struct to this one (for CPU aggregation)
    pub fn add(self: *CgroupMetrics, other: *const CgroupMetrics) void {
        self.cpu_time_ns += other.cpu_time_ns;
        self.memory_bytes += other.memory_bytes;
        self.net_rx_bytes += other.net_rx_bytes;
        self.net_tx_bytes += other.net_tx_bytes;
        self.net_rx_packets += other.net_rx_packets;
        self.net_tx_packets += other.net_tx_packets;
        self.block_read_bytes += other.block_read_bytes;
        self.block_write_bytes += other.block_write_bytes;
        self.block_read_ops += other.block_read_ops;
        self.block_write_ops += other.block_write_ops;
        if (other.timestamp > self.timestamp) {
            self.timestamp = other.timestamp;
        }
    }
};

// Collector configuration
pub const Config = struct {
    // Path to BPF object file (null to use embedded)
    bpf_object_path: ?[]const u8 = null,
    // Cgroup root path
    cgroup_root: ?[]const u8 = null,
    // Collection interval in milliseconds
    collection_interval_ms: u64 = 1000,
    // Container discovery interval in milliseconds
    discovery_interval_ms: u64 = 5000,
    // Enable debug logging
    debug: bool = false,
};

// Attached cgroup program info
const CgroupAttachment = struct {
    cgroup_fd: i32,
    ingress_prog_fd: i32,
    egress_prog_fd: i32,
};

// eBPF container metrics collector
pub const Collector = struct {
    allocator: Allocator,
    config: Config,
    tsdb: *TSDB,
    registry: *Registry,

    // BPF resources (libbpf)
    bpf_object: ?libbpf.Object,
    metrics_map_fd: i32,
    num_cpus: u32,
    percpu_buffer: ?[]CgroupMetrics,

    // Program file descriptors
    sched_switch_prog_fd: i32,
    mmap_prog_fd: i32,
    ingress_prog_fd: i32,
    egress_prog_fd: i32,

    // Attached links
    sched_switch_link: ?libbpf.Link,
    mmap_link: ?libbpf.Link,

    // Cgroup program attachments
    cgroup_attachments: std.AutoHashMap(u64, CgroupAttachment),

    // Container tracking
    container_detector: ?detector.Detector,
    cgroup_series: std.AutoHashMap(CgroupMetricKey, u64),
    rate_calculator: metrics_mod.RateCalculator,

    // State
    running: std.atomic.Value(bool),
    last_collection_ns: i128,
    last_discovery_ns: i128,

    // Stats
    collections: u64,
    points_inserted: u64,
    errors: u64,

    const CgroupMetricKey = struct {
        cgroup_id: u64,
        metric_type: MetricType,
    };

    const Self = @This();

    pub fn init(allocator: Allocator, tsdb: *TSDB, registry: *Registry, config: Config) !Self {
        return .{
            .allocator = allocator,
            .config = config,
            .tsdb = tsdb,
            .registry = registry,
            .bpf_object = null,
            .metrics_map_fd = -1,
            .num_cpus = 0,
            .percpu_buffer = null,
            .sched_switch_prog_fd = -1,
            .mmap_prog_fd = -1,
            .ingress_prog_fd = -1,
            .egress_prog_fd = -1,
            .sched_switch_link = null,
            .mmap_link = null,
            .cgroup_attachments = std.AutoHashMap(u64, CgroupAttachment).init(allocator),
            .container_detector = null,
            .cgroup_series = std.AutoHashMap(CgroupMetricKey, u64).init(allocator),
            .rate_calculator = metrics_mod.RateCalculator.init(allocator),
            .running = std.atomic.Value(bool).init(false),
            .last_collection_ns = 0,
            .last_discovery_ns = 0,
            .collections = 0,
            .points_inserted = 0,
            .errors = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();

        // Detach cgroup programs
        var it = self.cgroup_attachments.iterator();
        while (it.next()) |entry| {
            self.detachCgroupPrograms(entry.value_ptr.*);
        }
        self.cgroup_attachments.deinit();

        // Destroy links
        if (self.sched_switch_link) |link| link.destroy();
        if (self.mmap_link) |link| link.destroy();

        // Free per-CPU buffer
        if (self.percpu_buffer) |buf| {
            self.allocator.free(buf);
        }

        // Close BPF object
        if (self.bpf_object) |obj| obj.close();

        // Clean up detector
        if (self.container_detector) |*det| det.deinit();

        self.cgroup_series.deinit();
        self.rate_calculator.deinit();
        // Note: registry is owned by caller
    }

    // Embedded BPF objects - architecture-specific
    const embedded_bpf_x86 = @embedFile("../bpf/cgroupmon_x86_bpfel.o");
    const embedded_bpf_arm64 = @embedFile("../bpf/cgroupmon_arm64_bpfel.o");

    // Load BPF programs via libbpf
    pub fn load(self: *Self) !void {
        // Open BPF object
        if (self.config.bpf_object_path) |path| {
            // Load from file
            const path_z = try self.allocator.dupeZ(u8, path);
            defer self.allocator.free(path_z);
            self.bpf_object = try libbpf.Object.openFile(path_z);
        } else {
            // Use embedded BPF object based on target architecture
            const embedded = comptime blk: {
                const arch = @import("builtin").cpu.arch;
                break :blk if (arch == .x86_64)
                    embedded_bpf_x86
                else if (arch == .aarch64)
                    embedded_bpf_arm64
                else
                    @compileError("Unsupported architecture for eBPF collector");
            };
            self.bpf_object = try libbpf.Object.openMemory(embedded, "cgroupmon");
        }

        // Load into kernel
        try self.bpf_object.?.load();

        // Get map fd
        const map = self.bpf_object.?.findMap("cgroup_metrics_map") orelse {
            std.log.err("Map 'cgroup_metrics_map' not found in BPF object", .{});
            return error.MapNotFound;
        };
        self.metrics_map_fd = map.fd();

        // Allocate per-CPU buffer
        self.num_cpus = try libbpf.numPossibleCpus();
        self.percpu_buffer = try self.allocator.alloc(CgroupMetrics, self.num_cpus);

        // Get program FDs
        if (self.bpf_object.?.findProgramBySection("tp/sched/sched_switch")) |prog| {
            self.sched_switch_prog_fd = prog.fd();
        }
        if (self.bpf_object.?.findProgramBySection("tracepoint/syscalls/sys_enter_mmap")) |prog| {
            self.mmap_prog_fd = prog.fd();
        }
        if (self.bpf_object.?.findProgramBySection("cgroup_skb/ingress")) |prog| {
            self.ingress_prog_fd = prog.fd();
        }
        if (self.bpf_object.?.findProgramBySection("cgroup_skb/egress")) |prog| {
            self.egress_prog_fd = prog.fd();
        }

        // Attach tracepoints
        try self.attachTracepoints();

        // Initialize container detector
        self.container_detector = try detector.Detector.init(self.allocator, self.config.cgroup_root);

        std.log.info("eBPF collector loaded via libbpf (map_fd={d}, cpus={d})", .{ self.metrics_map_fd, self.num_cpus });
    }

    // Attach tracepoint programs
    fn attachTracepoints(self: *Self) !void {
        // Attach sched_switch
        if (self.bpf_object.?.findProgramBySection("tp/sched/sched_switch")) |prog| {
            self.sched_switch_link = prog.attachTracepoint("sched", "sched_switch") catch |err| {
                std.log.warn("Failed to attach sched_switch: {}", .{err});
                return;
            };
            std.log.info("Attached tp/sched/sched_switch", .{});
        }

        // Attach mmap tracepoint (optional)
        if (self.bpf_object.?.findProgramBySection("tracepoint/syscalls/sys_enter_mmap")) |prog| {
            self.mmap_link = prog.attachTracepoint("syscalls", "sys_enter_mmap") catch |err| {
                std.log.warn("Failed to attach sys_enter_mmap: {}", .{err});
                return;
            };
            std.log.info("Attached tracepoint/syscalls/sys_enter_mmap", .{});
        }
    }

    // Start the collector
    pub fn start(self: *Self) !void {
        if (self.bpf_object == null) {
            try self.load();
        }
        self.running.store(true, .release);
        std.log.info("eBPF collector started", .{});
    }

    // Stop the collector
    pub fn stop(self: *Self) void {
        self.running.store(false, .release);
    }

    // Run one collection cycle
    pub fn collect(self: *Self) !void {
        if (!self.running.load(.acquire)) return;

        const now = std.time.nanoTimestamp();

        // Periodic container discovery
        const discovery_interval_ns = @as(i128, self.config.discovery_interval_ms) * 1_000_000;
        if (now - self.last_discovery_ns >= discovery_interval_ns) {
            try self.discoverContainers();
            self.last_discovery_ns = now;
        }

        // Collection
        const collection_interval_ns = @as(i128, self.config.collection_interval_ms) * 1_000_000;
        if (now - self.last_collection_ns >= collection_interval_ns) {
            try self.collectMetrics();
            self.last_collection_ns = now;
            self.collections += 1;
        }
    }

    // Discover new containers
    fn discoverContainers(self: *Self) !void {
        var det = &self.container_detector.?;
        var result = try det.scan();
        defer result.deinit();

        // Handle new containers
        for (result.new_containers.items) |*c| {
            std.log.info("Discovered container: {s} ({s})", .{ c.shortIdStr(), c.runtime.toString() });

            // Register series in shared registry
            try self.registerContainerSeries(c.cgroup_id, c.shortIdStr());

            // Attach cgroup programs
            try self.attachCgroupPrograms(c.*);
        }

        // Handle removed containers
        for (result.removed_cgroup_ids.items) |cgroup_id| {
            std.log.info("Container removed: cgroup_id={}", .{cgroup_id});

            // Detach cgroup programs
            if (self.cgroup_attachments.fetchRemove(cgroup_id)) |entry| {
                self.detachCgroupPrograms(entry.value);
            }

            // Remove cgroup-to-series mapping (registry keeps the series for historical queries)
            for (MetricType.all()) |metric| {
                _ = self.cgroup_series.remove(.{ .cgroup_id = cgroup_id, .metric_type = metric });
            }

            // Remove from rate calculator
            self.rate_calculator.remove(cgroup_id);
        }
    }

    // Register all metric series for a container
    fn registerContainerSeries(self: *Self, cgroup_id: u64, container_id: []const u8) !void {
        for (MetricType.all()) |metric| {
            // Build series name
            const series_name = try std.fmt.allocPrint(
                self.allocator,
                "{s}.{s}",
                .{ metric.name(), container_id },
            );
            defer self.allocator.free(series_name);

            // Generate deterministic series ID
            const series_id = registry_mod.fnvHash(series_name);

            // Register in shared registry
            try self.registry.registerWithId(series_id, series_name, .ebpf_container, container_id);

            // Cache cgroup-to-series mapping for fast lookup during collection
            try self.cgroup_series.put(.{ .cgroup_id = cgroup_id, .metric_type = metric }, series_id);
        }
    }

    // Attach cgroup SKB programs to a container
    fn attachCgroupPrograms(self: *Self, cont: Container) !void {
        // Open cgroup directory
        const cgroup_fd = std.posix.open(cont.cgroup_path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0) catch |err| {
            std.log.warn("Failed to open cgroup {s}: {}", .{ cont.cgroup_path, err });
            return;
        };

        const attachment = CgroupAttachment{
            .cgroup_fd = @intCast(cgroup_fd),
            .ingress_prog_fd = self.ingress_prog_fd,
            .egress_prog_fd = self.egress_prog_fd,
        };

        // Attach ingress program
        if (self.ingress_prog_fd >= 0) {
            libbpf.progAttachCgroup(@intCast(cgroup_fd), self.ingress_prog_fd, 0) catch |err| {
                std.log.warn("Failed to attach cgroup_skb/ingress: {}", .{err});
            };
        }

        // Attach egress program
        if (self.egress_prog_fd >= 0) {
            libbpf.progAttachCgroup(@intCast(cgroup_fd), self.egress_prog_fd, 1) catch |err| {
                std.log.warn("Failed to attach cgroup_skb/egress: {}", .{err});
            };
        }

        try self.cgroup_attachments.put(cont.cgroup_id, attachment);
    }

    // Detach cgroup programs from a container
    fn detachCgroupPrograms(_: *Self, attachment: CgroupAttachment) void {
        if (attachment.ingress_prog_fd >= 0) {
            libbpf.progDetachCgroup(attachment.cgroup_fd, attachment.ingress_prog_fd, 0) catch {};
        }
        if (attachment.egress_prog_fd >= 0) {
            libbpf.progDetachCgroup(attachment.cgroup_fd, attachment.egress_prog_fd, 1) catch {};
        }
        _ = std.c.close(attachment.cgroup_fd);
    }

    // Collect metrics from BPF maps and insert into TSDB
    fn collectMetrics(self: *Self) !void {
        var all_metrics = try self.readAllMetrics();
        defer all_metrics.deinit();

        const timestamp = std.time.milliTimestamp();

        // Process each container's metrics
        var it = all_metrics.iterator();
        while (it.next()) |entry| {
            const cgroup_id = entry.key_ptr.*;
            const raw_metrics = entry.value_ptr;

            // Calculate rates
            const rates_opt = self.rate_calculator.calculate(cgroup_id, raw_metrics);
            const rates = rates_opt orelse continue; // Skip first sample

            // Insert each metric type
            for (MetricType.all()) |metric| {
                const key = CgroupMetricKey{ .cgroup_id = cgroup_id, .metric_type = metric };
                if (self.cgroup_series.get(key)) |sid| {
                    const value = rates.getValue(metric);
                    if (self.tsdb.insert(sid, timestamp, value)) {
                        self.points_inserted += 1;
                    } else {
                        if (self.config.debug) {
                            std.log.warn("Failed to insert metric (buffer full)", .{});
                        }
                        self.errors += 1;
                    }
                }
            }
        }
    }

    // Read all metrics from BPF map with per-CPU aggregation
    fn readAllMetrics(self: *Self) !std.AutoHashMap(u64, CgroupMetrics) {
        var results = std.AutoHashMap(u64, CgroupMetrics).init(self.allocator);
        errdefer results.deinit();

        var key: u64 = undefined;
        var next_key: u64 = undefined;

        // Get first key
        libbpf.mapGetNextKey(self.metrics_map_fd, null, &next_key) catch {
            return results; // Empty map
        };

        const c = @cImport({
            @cInclude("bpf/bpf.h");
        });

        while (true) {
            key = next_key;

            // Read per-CPU values
            const ret = c.bpf_map_lookup_elem(self.metrics_map_fd, &key, @ptrCast(self.percpu_buffer.?.ptr));
            if (ret == 0) {
                // Aggregate across CPUs
                var aggregated = CgroupMetrics.zero();
                for (self.percpu_buffer.?[0..self.num_cpus]) |*cpu_val| {
                    aggregated.add(cpu_val);
                }
                try results.put(key, aggregated);
            }

            // Get next key
            libbpf.mapGetNextKey(self.metrics_map_fd, &key, &next_key) catch break;
        }

        return results;
    }

    // Get collector statistics
    pub fn getStats(self: *Self) Stats {
        return .{
            .collections = self.collections,
            .points_inserted = self.points_inserted,
            .errors = self.errors,
            .containers = if (self.container_detector) |det| det.containerCount() else 0,
            .series = self.registry.count(),
        };
    }

    pub const Stats = struct {
        collections: u64,
        points_inserted: u64,
        errors: u64,
        containers: usize,
        series: usize,
    };
};

// Run the collector in a loop
pub fn runCollectorLoop(collector: *Collector) void {
    while (collector.running.load(.acquire)) {
        collector.collect() catch |err| {
            std.log.err("Collection error: {}", .{err});
        };

        // Sleep until next collection
        std.Thread.sleep(collector.config.collection_interval_ms * 1_000_000);
    }
}

test "collector config defaults" {
    const config = Config{};
    try std.testing.expectEqual(@as(u64, 1000), config.collection_interval_ms);
    try std.testing.expectEqual(@as(u64, 5000), config.discovery_interval_ms);
}
