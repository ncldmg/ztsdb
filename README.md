# ztsdb

Time Series Database with eBPF metrics collection for containers (for now), written in Zig.

## Requirements

- Zig 0.14+
- libbpf, libelf, zlib (`sudo pacman -S libbpf libelf zlib` or `sudo apt install libbpf-dev libelf-dev zlib1g-dev`)
- Linux kernel 5.8+ with BTF support
- Root privileges (for eBPF)

## Build

```bash
zig build
```

## Quick Start

### 1. Start the collector with HTTP API

```bash
sudo ./zig-out/bin/tsvdb collect --web-port 8080
```

### 2. Run a container to generate metrics

```bash
# Run a container that does some work
docker run --rm -d --name test-container alpine sh -c "while true; do dd if=/dev/zero of=/dev/null bs=1M count=100; sleep 1; done"
```

### 3. Check metrics via HTTP API

```bash
# List all series
curl http://localhost:8080/series

# Get server stats
curl http://localhost:8080/stats

# API help
curl http://localhost:8080/
```

### 4. Cleanup

```bash
docker stop test-container
# Ctrl+C to stop the collector
```

## CLI Commands

```bash
# Start TSDB server (TCP protocol)
./zig-out/bin/tsvdb serve --port 9876

# Start eBPF collector with HTTP API
sudo ./zig-out/bin/tsvdb collect --web-port 8080

# Generate test data
./zig-out/bin/tsvdb generate --host 127.0.0.1 --port 9876 --count 1000
```

## Collector Options

| Option | Description | Default |
|--------|-------------|---------|
| `--web-port` | HTTP API port | disabled |
| `--port` | Internal TSDB port | 9876 |
| `--interval` | Collection interval (ms) | 1000 |
| `--discovery-interval` | Container discovery interval (ms) | 5000 |
| `--cgroup-root` | Cgroup v2 root path | /sys/fs/cgroup |
| `--debug` | Enable debug logging | false |

## HTTP API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | API help |
| `GET /stats` | Server statistics |
| `GET /series` | List all registered series |
| `GET /query?series=ID&start=TS&end=TS` | Query data points |

## TCP Binary Protocol

For high-performance direct ingestion, connect to the TSDB server via TCP.

### Start the server

```bash
./zig-out/bin/tsvdb serve --port 9876
```

### Protocol Format

All messages use a 16-byte header + payload:

```
Header (16 bytes):
┌──────────┬─────────┬──────────┬────────────┬──────────┐
│ Magic    │ Version │ MsgType  │ PayloadLen │ Reserved │
│ "ZTSD"   │ 0x01    │ 1 byte   │ 4 bytes LE │ 6 bytes  │
└──────────┴─────────┴──────────┴────────────┴──────────┘

Data Point (24 bytes):
┌───────────┬───────────┬───────────┐
│ series_id │ timestamp │ value     │
│ u64 LE    │ i64 LE    │ f64 LE    │
└───────────┴───────────┴───────────┘
```

### Message Types

| Type | Code | Description |
|------|------|-------------|
| insert | 0x01 | Insert single data point |
| insert_batch | 0x02 | Insert multiple data points |
| query | 0x10 | Query time range |
| ping | 0x20 | Health check |
| ok | 0x80 | Success response |
| error | 0x81 | Error response |
| data | 0x82 | Query results |

### Example: Insert a data point

```bash
# Using the built-in client
./zig-out/bin/tsvdb generate --port 9876 --series 12345 --count 100

# Or with netcat (insert single point):
# Header: ZTSD + version(1) + type(0x01) + payload_len(24) + reserved(6)
# Payload: series_id(8) + timestamp(8) + value(8)
printf 'ZTSD\x01\x01\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x94\x87\x97\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00Y@' | nc localhost 9876
```

### Example: Using the Zig client

```zig
const Client = @import("ztsdb").Client;

var client = try Client.init(allocator, .{ .host = "127.0.0.1", .port = 9876 });
defer client.deinit();

try client.connect();

// insert(series_id: u64, timestamp_ms: i64, value: f64)
try client.insert(12345, std.time.milliTimestamp(), 42.5);
```

## Metrics Collected

Per container (10 metrics):
- `container.cpu.usage` - CPU usage percentage
- `container.memory.bytes` - Memory usage
- `container.net.rx.bytes` - Network receive bytes/sec
- `container.net.tx.bytes` - Network transmit bytes/sec
- `container.net.rx.packets` - Network receive packets/sec
- `container.net.tx.packets` - Network transmit packets/sec
- `container.block.read.bytes` - Block read bytes/sec
- `container.block.write.bytes` - Block write bytes/sec
- `container.block.read.ops` - Block read ops/sec
- `container.block.write.ops` - Block write ops/sec
