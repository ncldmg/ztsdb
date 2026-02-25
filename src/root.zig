const std = @import("std");

// Core TSDB modules
pub const chunk = @import("timeserie/chunk.zig");
pub const wal = @import("timeserie/wal.zig");
pub const tsdb = @import("timeserie/tsdb.zig");
pub const lockfree_buffer = @import("timeserie/lockfree_buffer.zig");
pub const mmap_chunk = @import("timeserie/mmap_chunk.zig");
pub const cold_storage = @import("timeserie/cold_storage.zig");
pub const tiered_storage = @import("timeserie/tiered_storage.zig");
pub const protocol = @import("protocol/protocol.zig");
pub const server = @import("server/server.zig");
pub const client = @import("client/client.zig");
pub const http = @import("web/http.zig");

// eBPF integration modules
pub const ebpf = struct {
    pub const libbpf = @import("ebpf/libbpf.zig");
};

pub const cgroup = struct {
    pub const container = @import("cgroup/container.zig");
    pub const detector = @import("cgroup/detector.zig");
};

pub const collector = struct {
    pub const main = @import("collector/collector.zig");
    pub const series = @import("collector/series.zig");
    pub const metrics = @import("collector/metrics.zig");
};

// Re-exported types for convenience
pub const Chunk = chunk.Chunk;
pub const ChunkPool = chunk.ChunkPool;
pub const WAL = wal.WAL;
pub const TSDB = tsdb.TSDB;
pub const LockFreeBuffer = lockfree_buffer.LockFreeBuffer;
pub const MmapChunk = mmap_chunk.MmapChunk;
pub const ColdStorage = cold_storage.ColdStorage;
pub const TieredStorage = tiered_storage.TieredStorage;
pub const Protocol = protocol.Protocol;
pub const DataPoint = protocol.DataPoint;
pub const Server = server.Server;
pub const Client = client.Client;
pub const HttpServer = http.HttpServer;

// eBPF types
pub const Collector = collector.main.Collector;
pub const CgroupMetrics = collector.main.CgroupMetrics;
pub const Container = cgroup.container.Container;
pub const ContainerRuntime = cgroup.container.ContainerRuntime;

// Import tests
test {
    std.testing.refAllDecls(@This());
    _ = chunk;
    _ = wal;
    _ = tsdb;
    _ = lockfree_buffer;
    _ = mmap_chunk;
    _ = cold_storage;
    _ = tiered_storage;
    _ = protocol;
    _ = server;
    _ = client;
    _ = http;
    // eBPF modules
    _ = ebpf.libbpf;
    _ = cgroup.container;
    _ = cgroup.detector;
    _ = collector.main;
    _ = collector.series;
    _ = collector.metrics;
}
