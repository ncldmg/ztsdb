# eBPF Collector to TSDB Data Flow

## Overview

The ztsdb eBPF collector monitors container resource usage using Linux eBPF programs and stores metrics in a high-performance time series database. This document describes the complete data flow from kernel-space collection to persistent storage.

## Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              KERNEL SPACE                                 │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │
│  │   Tracepoints    │    │   Cgroup SKB     │    │   Cgroup SKB     │     │
│  │  sched_switch    │    │     Ingress      │    │     Egress       │     │
│  │  sys_enter_mmap  │    │                  │    │                  │     │
│  └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘     │
│           │                       │                       │               │
│           └───────────────────────┼───────────────────────┘               │
│                                   ▼                                       │
│                    ┌──────────────────────────────┐                       │
│                    │    BPF Per-CPU Hash Map      │                       │
│                    │    cgroup_metrics_map        │                       │
│                    │    key: cgroup_id (u64)      │                       │
│                    │    value: CgroupMetrics      │                       │
│                    └──────────────────────────────┘                       │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ bpf() syscall
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                              USER SPACE                                   │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                         COLLECTOR                                  │   │
│  │                                                                    │   │
│  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐               │   │
│  │  │  Container  │   │  MapReader  │   │    Rate     │               │   │
│  │  │  Detector   │   │  (per-CPU   │   │ Calculator  │               │   │
│  │  │             │   │  aggregate) │   │             │               │   │
│  │  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘               │   │
│  │         │                 │                 │                      │   │
│  │         └─────────────────┼─────────────────┘                      │   │
│  │                           ▼                                        │   │
│  │                 ┌─────────────────┐                                │   │
│  │                 │ Series Registry │                                │   │
│  │                 │ name → hash ID  │                                │   │
│  │                 └────────┬────────┘                                │   │
│  │                          │                                         │   │
│  └──────────────────────────┼─────────────────────────────────────────┘   │
│                             │                                             │
│                             ▼                                             │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                            TSDB                                    │   │
│  │                                                                    │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │              Lock-Free Ingestion Layer                      │   │   │
│  │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │   │   │
│  │  │  │Partition0│  │Partition1│  │Partition2│  │Partition3│     │   │   │
│  │  │  │  Buffer  │  │  Buffer  │  │  Buffer  │  │  Buffer  │     │   │   │
│  │  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │   │   │
│  │  │       │             │             │             │           │   │   │
│  │  │       └─────────────┼─────────────┼─────────────┘           │   │   │
│  │  │                     ▼             ▼                         │   │   │
│  │  │              Consumer Threads (drain buffers)               │   │   │
│  │  └─────────────────────┬───────────────────────────────────────┘   │   │
│  │                        │                                           │   │
│  │                        ▼                                           │   │
│  │  ┌─────────────────────────────────────────────────────────────┐   │   │
│  │  │                   Storage Layer                             │   │   │
│  │  │                                                             │   │   │
│  │  │   ┌─────────┐      ┌─────────┐      ┌─────────┐             │   │   │
│  │  │   │  Hot    │ ───► │  Warm   │ ───► │  Cold   │             │   │   │
│  │  │   │ Chunks  │      │ (mmap)  │      │(columnar)│            │   │   │
│  │  │   └─────────┘      └─────────┘      └─────────┘             │   │   │
│  │  │        │                                                    │   │   │
│  │  │        ▼                                                    │   │   │
│  │  │   ┌─────────┐                                               │   │   │
│  │  │   │   WAL   │                                               │   │   │
│  │  │   └─────────┘                                               │   │   │
│  │  └─────────────────────────────────────────────────────────────┘   │   │
│  │                                                                    │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. eBPF Programs (Kernel Space)

**Location:** `src/bpf/cgroupmon_*.o` (pre-compiled)

The collector uses three types of eBPF programs:

| Program | Attach Point | Data Collected |
|---------|--------------|----------------|
| `tp/sched/sched_switch` | Tracepoint | CPU time per cgroup |
| `tp/syscalls/sys_enter_mmap` | Tracepoint | Memory allocations |
| `cgroup_skb/ingress` | Cgroup | Network RX bytes/packets |
| `cgroup_skb/egress` | Cgroup | Network TX bytes/packets |

**BPF Map Structure:**

```zig
// src/collector/collector.zig
pub const CgroupMetrics = extern struct {
    cpu_time_ns: u64,        // Cumulative CPU nanoseconds
    memory_bytes: u64,       // Current memory usage
    net_rx_bytes: u64,       // Cumulative bytes received
    net_tx_bytes: u64,       // Cumulative bytes transmitted
    net_rx_packets: u64,     // Cumulative packets received
    net_tx_packets: u64,     // Cumulative packets transmitted
    block_read_bytes: u64,   // Cumulative block reads
    block_write_bytes: u64,  // Cumulative block writes
    block_read_ops: u64,     // Cumulative read operations
    block_write_ops: u64,    // Cumulative write operations
    timestamp: u64,          // Last update timestamp
};
```

The map is **per-CPU** to avoid contention - each CPU core maintains its own copy of the metrics.

### 2. libbpf Integration (src/ebpf/libbpf.zig)

Wraps the libbpf C library for BPF program loading and management:
- Loading BPF objects from ELF files or embedded bytes
- Automatic map creation from BTF definitions
- Program attachment to tracepoints and cgroups
- Per-CPU map reading with aggregation

**Key Flow:**
```
ELF Object → libbpf.Object.open() → load() → findMap/findProgram → attach
```

**Dependencies:** Links with `-lbpf -lelf -lz`

### 3. Container Detector (src/cgroup/detector.zig)

Walks the cgroup v2 hierarchy (`/sys/fs/cgroup`) to discover running containers:

```
/sys/fs/cgroup
├── docker/
│   └── <container_id>/     ← Docker containers
├── podman-<container_id>.scope/  ← Podman containers
└── system.slice/
    └── containerd-<id>.scope/    ← containerd
```

**Detection Logic:**
1. Recursively walk cgroup directories
2. Pattern match against known container runtime paths
3. Extract container ID from path
4. Get cgroup inode number (used as stable cgroup_id)
5. Track new/removed containers between scans

### 4. Map Reader (src/collector/collector.zig)

Reads metrics from the per-CPU BPF hash map using libbpf and aggregates:

```zig
fn readAllMetrics(self: *Self) !std.AutoHashMap(u64, CgroupMetrics) {
    // Uses libbpf's bpf_map_lookup_elem and bpf_map_get_next_key
    // For each key (cgroup_id):
    //   1. Read values from all CPUs into percpu_buffer
    //   2. Sum counters across CPUs via CgroupMetrics.add()
    //   3. Take latest timestamp
    //   4. Return aggregated metrics
}
```

The collector maintains a `percpu_buffer` sized for `num_cpus` entries, allocated via `libbpf.numPossibleCpus()`.

### 5. Rate Calculator (src/collector/metrics.zig)

Converts cumulative counters to rates:

```zig
pub const RateMetrics = struct {
    cpu_percent: f64,              // (delta_ns / elapsed_ns) * 100
    memory_bytes: u64,             // Absolute value (not a rate)
    net_rx_bytes_per_sec: f64,     // delta / elapsed_seconds
    net_tx_bytes_per_sec: f64,
    // ... etc
};
```

**Algorithm:**
1. Store previous sample for each cgroup_id
2. On new sample: `rate = (current - previous) / time_delta`
3. Handle counter wraps/resets gracefully
4. Minimum time delta of 100ms to avoid division issues

### 6. Series Registry (src/metrics/registry.zig)

Unified registry for all metric sources (eBPF, OTLP, manual). Maps metric names to series IDs using FNV-1a hash:

```zig
// Metric naming: <metric_type>.<container_short_id>
// Example: "container.cpu.usage.abc123def456"

pub fn fnvHash(data: []const u8) u64 {
    // FNV-1a 64-bit hash for deterministic series IDs
}

// Registry tracks:
// - series_id → SeriesInfo (name, source, metadata)
// - name → series_id mapping
// - Thread-safe via mutex
```

**10 Metric Types Per Container:**
- `container.cpu.usage`
- `container.memory.bytes`
- `container.net.rx.bytes`
- `container.net.tx.bytes`
- `container.net.rx.packets`
- `container.net.tx.packets`
- `container.block.read.bytes`
- `container.block.write.bytes`
- `container.block.read.ops`
- `container.block.write.ops`

### 7. TSDB Ingestion (src/timeserie/tsdb.zig)

**Lock-Free Buffer Architecture:**

```
Producer Thread(s)          Consumer Thread(s)
      │                           │
      ▼                           ▼
┌──────────────────────────────────────┐
│  Lock-Free Ring Buffer (per partition) │
│  ┌────┬────┬────┬────┬────┬────┐    │
│  │ D0 │ D1 │ D2 │ D3 │ D4 │ D5 │    │
│  └────┴────┴────┴────┴────┴────┘    │
│       ▲                    ▲         │
│       │                    │         │
│      tail                head        │
└──────────────────────────────────────┘
```

**Partitioning Strategy:**
- `partition_idx = series_id % num_partitions`
- Each partition has its own buffer and chunk map
- Eliminates cross-partition contention

**Data Flow:**
1. `insert(series_id, timestamp, value)` → push to partition buffer
2. Consumer thread drains buffer in batches (4096 points)
3. Points written to partition-local chunks
4. Full chunks flushed to warm storage (mmap files)

### 8. Storage Tiers

| Tier | Storage | Use Case | Compaction |
|------|---------|----------|------------|
| Hot | In-memory chunks | Recent data, high-speed queries | → Warm when full |
| Warm | Memory-mapped files | Hours-old data | → Cold periodically |
| Cold | Columnar files | Historical data | Archive/delete |

## Collection Loop

```zig
// src/collector/collector.zig
pub fn collect(self: *Self) !void {
    // 1. Periodic container discovery (every 5s default)
    if (now - self.last_discovery_ns >= discovery_interval) {
        try self.discoverContainers();  // Attaches cgroup SKB programs
    }

    // 2. Read metrics from BPF map via libbpf (every 1s default)
    var all_metrics = try self.readAllMetrics();  // Per-CPU aggregation
    defer all_metrics.deinit();

    // 3. For each container's metrics
    var it = all_metrics.iterator();
    while (it.next()) |entry| {
        const cgroup_id = entry.key_ptr.*;
        const raw_metrics = entry.value_ptr;

        // 4. Calculate rates from raw counters
        const rates = self.rate_calculator.calculate(cgroup_id, raw_metrics) orelse continue;

        // 5. Insert each metric type into TSDB via series registry
        for (MetricType.all()) |metric| {
            if (self.cgroup_series.get(.{ .cgroup_id = cgroup_id, .metric_type = metric })) |series_id| {
                const value = rates.getValue(metric);
                _ = self.tsdb.insert(series_id, timestamp, value);
            }
        }
    }
}
```

## Data Point Structure

```zig
pub const DataPoint = struct {
    series_id: u64,    // FNV-1a hash of metric name
    timestamp: i64,    // Milliseconds since epoch
    value: f64,        // Metric value
};
```

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Collection interval | 1 second (configurable) |
| Discovery interval | 5 seconds (configurable) |
| Buffer capacity | 65,536 points (default) |
| Partitions | 4 (default) |
| Chunk size | 8,192 points |
| Drain batch size | 4,096 points |

## Cgroup Program Attachment

For each discovered container, cgroup SKB programs are attached:

```zig
fn attachCgroupPrograms(self: *Self, container: Container) !void {
    // Open cgroup directory
    const cgroup_fd = open(container.cgroup_path, O_RDONLY);

    // Attach network ingress program
    bpf_prog_attach(cgroup_fd, prog_fd, BPF_CGROUP_INET_INGRESS);

    // Attach network egress program
    bpf_prog_attach(cgroup_fd, prog_fd, BPF_CGROUP_INET_EGRESS);
}
```

## Error Handling

- **Buffer full**: Returns false, caller can retry with backoff
- **Map read failure**: Skip key, continue iteration
- **Container disappeared**: Graceful cleanup, unregister series
- **Counter wrap**: Treat as reset, use current value

## Query Path

```
HTTP Request → /query?series=container.cpu.usage.abc123
                           │
                           ▼
                    Series Registry
                    (name → series_id)
                           │
                           ▼
                    TSDB.query(series_id, start, end)
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
        Hot Chunks    Lock-Free      Tiered Storage
                       Buffer         (warm/cold)
            │              │              │
            └──────────────┼──────────────┘
                           ▼
                    Merged Results
```
