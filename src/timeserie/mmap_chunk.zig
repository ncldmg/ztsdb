const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const posix = std.posix;
const Allocator = std.mem.Allocator;

// Memory-mapped chunk file for warm tier storage.
// Provides zero-copy access to chunk data via mmap.
// File format:
//   Header (64 bytes):
//     - magic: u32 = 0x5A544348 ("ZTCH")
//     - version: u32 = 1
//     - series_id: u64
//     - count: u64
//     - min_ts: i64
//     - max_ts: i64
//     - checksum: u32
//     - reserved: 20 bytes
//   Data:
//     - timestamps: [count]i64 (aligned to 64 bytes)
//     - values: [count]f64 (aligned to 64 bytes)
pub const MmapChunk = struct {
    allocator: Allocator,
    file: ?fs.File,
    path: []const u8,
    mmap_data: ?[]align(std.heap.page_size_min) u8,

    // Parsed header
    series_id: u64,
    count: usize,
    min_ts: i64,
    max_ts: i64,

    // Data pointers (into mmap region)
    timestamps: []const i64,
    values: []const f64,

    const MAGIC: u32 = 0x5A544348; // "ZTCH"
    const VERSION: u32 = 1;
    const HEADER_SIZE: usize = 64;
    const DATA_ALIGNMENT: usize = 64;

    pub const Header = extern struct {
        magic: u32,
        version: u32,
        series_id: u64,
        count: u64,
        min_ts: i64,
        max_ts: i64,
        checksum: u32,
        _reserved: [20]u8,
    };

    comptime {
        if (@sizeOf(Header) != HEADER_SIZE) {
            @compileError("Header size mismatch");
        }
    }

    pub const Error = error{
        InvalidMagic,
        InvalidVersion,
        ChecksumMismatch,
        FileTooSmall,
        InvalidArgument,
    } || fs.File.OpenError || posix.MMapError || Allocator.Error;

    // Open an existing mmap'd chunk file
    pub fn open(allocator: Allocator, dir: fs.Dir, path: []const u8) !MmapChunk {
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        const file = try dir.openFile(path, .{ .mode = .read_only });
        errdefer file.close();

        const stat = try file.stat();
        if (stat.size < HEADER_SIZE) {
            return error.FileTooSmall;
        }

        const mmap_data = try posix.mmap(
            null,
            stat.size,
            posix.PROT.READ,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        // Parse header
        const header: *const Header = @ptrCast(mmap_data.ptr);

        if (header.magic != MAGIC) {
            posix.munmap(mmap_data);
            return error.InvalidMagic;
        }

        if (header.version != VERSION) {
            posix.munmap(mmap_data);
            return error.InvalidVersion;
        }

        const count: usize = @intCast(header.count);

        // Calculate data offsets (aligned)
        const ts_offset = alignUp(HEADER_SIZE, DATA_ALIGNMENT);
        const ts_size = count * @sizeOf(i64);
        const val_offset = alignUp(ts_offset + ts_size, DATA_ALIGNMENT);

        // Get slices into mmap region
        const base: [*]const u8 = mmap_data.ptr;
        const timestamps: []const i64 = @as([*]const i64, @ptrCast(@alignCast(base + ts_offset)))[0..count];
        const values: []const f64 = @as([*]const f64, @ptrCast(@alignCast(base + val_offset)))[0..count];

        return MmapChunk{
            .allocator = allocator,
            .file = file,
            .path = path_copy,
            .mmap_data = mmap_data,
            .series_id = header.series_id,
            .count = count,
            .min_ts = header.min_ts,
            .max_ts = header.max_ts,
            .timestamps = timestamps,
            .values = values,
        };
    }

    // Create a new mmap'd chunk file from in-memory data
    pub fn create(
        allocator: Allocator,
        dir: fs.Dir,
        path: []const u8,
        series_id: u64,
        timestamps: []const i64,
        values: []const f64,
        min_ts: i64,
        max_ts: i64,
    ) !MmapChunk {
        const count = timestamps.len;
        if (count != values.len) {
            return error.InvalidArgument;
        }

        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        // Calculate file size
        const ts_offset = alignUp(HEADER_SIZE, DATA_ALIGNMENT);
        const ts_size = count * @sizeOf(i64);
        const val_offset = alignUp(ts_offset + ts_size, DATA_ALIGNMENT);
        const val_size = count * @sizeOf(f64);
        const file_size = alignUp(val_offset + val_size, DATA_ALIGNMENT);

        // Create file
        const file = try dir.createFile(path, .{ .read = true });
        errdefer file.close();

        // Set file size
        try file.setEndPos(file_size);

        // mmap for writing
        const mmap_data = try posix.mmap(
            null,
            file_size,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        // Write header
        const header: *Header = @ptrCast(mmap_data.ptr);
        header.* = .{
            .magic = MAGIC,
            .version = VERSION,
            .series_id = series_id,
            .count = count,
            .min_ts = min_ts,
            .max_ts = max_ts,
            .checksum = 0, // TODO: compute checksum
            ._reserved = [_]u8{0} ** 20,
        };

        // Write data
        const base: [*]u8 = mmap_data.ptr;
        const ts_dest: [*]i64 = @ptrCast(@alignCast(base + ts_offset));
        const val_dest: [*]f64 = @ptrCast(@alignCast(base + val_offset));

        @memcpy(ts_dest[0..count], timestamps);
        @memcpy(val_dest[0..count], values);

        // Sync to disk
        try posix.msync(mmap_data, posix.MSF.SYNC);

        // Get read-only slices
        const timestamps_slice: []const i64 = ts_dest[0..count];
        const values_slice: []const f64 = val_dest[0..count];

        return MmapChunk{
            .allocator = allocator,
            .file = file,
            .path = path_copy,
            .mmap_data = mmap_data,
            .series_id = series_id,
            .count = count,
            .min_ts = min_ts,
            .max_ts = max_ts,
            .timestamps = timestamps_slice,
            .values = values_slice,
        };
    }

    pub fn deinit(self: *MmapChunk) void {
        if (self.mmap_data) |data| {
            posix.munmap(data);
            self.mmap_data = null;
        }
        if (self.file) |file| {
            file.close();
            self.file = null;
        }
        self.allocator.free(self.path);
    }

    // Query data points in time range
    pub fn query(self: *const MmapChunk, allocator: Allocator, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
        // Quick check: does range overlap?
        if (end_ts < self.min_ts or start_ts > self.max_ts) {
            return;
        }

        // Binary search for start position
        const start_idx = self.lowerBound(start_ts);

        // Scan from start_idx
        for (self.timestamps[start_idx..], self.values[start_idx..]) |ts, val| {
            if (ts > end_ts) break;
            if (ts >= start_ts) {
                try result.append(allocator, .{
                    .series_id = self.series_id,
                    .timestamp = ts,
                    .value = val,
                });
            }
        }
    }

    // Binary search for first timestamp >= target
    fn lowerBound(self: *const MmapChunk, target: i64) usize {
        var left: usize = 0;
        var right: usize = self.count;

        while (left < right) {
            const mid = left + (right - left) / 2;
            if (self.timestamps[mid] < target) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left;
    }

    // Advise kernel about access pattern
    pub fn prefetch(self: *MmapChunk) void {
        if (self.mmap_data) |data| {
            posix.madvise(data, posix.MADV.WILLNEED) catch {};
        }
    }

    // Tell kernel we're done with this for now
    pub fn release(self: *MmapChunk) void {
        if (self.mmap_data) |data| {
            posix.madvise(data, posix.MADV.DONTNEED) catch {};
        }
    }

    // Delete the underlying file
    pub fn delete(self: *MmapChunk, dir: fs.Dir) !void {
        const path = self.path;
        self.deinit();
        try dir.deleteFile(path);
    }
};

pub const DataPoint = @import("../protocol/protocol.zig").DataPoint;

fn alignUp(value: usize, alignment: usize) usize {
    return (value + alignment - 1) & ~(alignment - 1);
}

// Tests

test "MmapChunk create and open" {
    const allocator = testing.allocator;
    const dir = fs.cwd();
    const path = "/tmp/test_mmap_chunk.dat";

    // Create test data
    const timestamps = [_]i64{ 1000, 2000, 3000, 4000, 5000 };
    const values = [_]f64{ 1.1, 2.2, 3.3, 4.4, 5.5 };

    // Create mmap chunk
    {
        var chunk = try MmapChunk.create(
            allocator,
            dir,
            path,
            42,
            &timestamps,
            &values,
            1000,
            5000,
        );
        defer chunk.deinit();

        try testing.expectEqual(@as(u64, 42), chunk.series_id);
        try testing.expectEqual(@as(usize, 5), chunk.count);
        try testing.expectEqual(@as(i64, 1000), chunk.min_ts);
        try testing.expectEqual(@as(i64, 5000), chunk.max_ts);
    }

    // Re-open and verify
    {
        var chunk = try MmapChunk.open(allocator, dir, path);
        defer chunk.deinit();

        try testing.expectEqual(@as(u64, 42), chunk.series_id);
        try testing.expectEqual(@as(usize, 5), chunk.count);
        try testing.expectEqual(@as(i64, 2000), chunk.timestamps[1]);
        try testing.expectEqual(@as(f64, 3.3), chunk.values[2]);
    }

    // Cleanup
    try dir.deleteFile(path);
}

test "MmapChunk query" {
    const allocator = testing.allocator;
    const dir = fs.cwd();
    const path = "/tmp/test_mmap_query.dat";

    const timestamps = [_]i64{ 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000 };
    const values = [_]f64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    var chunk = try MmapChunk.create(allocator, dir, path, 1, &timestamps, &values, 1000, 10000);
    defer chunk.deinit();
    defer dir.deleteFile(path) catch {};

    // Query range
    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try chunk.query(allocator, 2500, 7500, &result);

    try testing.expectEqual(@as(usize, 5), result.items.len);
    try testing.expectEqual(@as(i64, 3000), result.items[0].timestamp);
    try testing.expectEqual(@as(i64, 7000), result.items[4].timestamp);
}

test "MmapChunk query out of range" {
    const allocator = testing.allocator;
    const dir = fs.cwd();
    const path = "/tmp/test_mmap_oor.dat";

    const timestamps = [_]i64{ 1000, 2000, 3000 };
    const values = [_]f64{ 1, 2, 3 };

    var chunk = try MmapChunk.create(allocator, dir, path, 1, &timestamps, &values, 1000, 3000);
    defer chunk.deinit();
    defer dir.deleteFile(path) catch {};

    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    // Query before range
    try chunk.query(allocator, 0, 500, &result);
    try testing.expectEqual(@as(usize, 0), result.items.len);

    // Query after range
    try chunk.query(allocator, 5000, 6000, &result);
    try testing.expectEqual(@as(usize, 0), result.items.len);
}
