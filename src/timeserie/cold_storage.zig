const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const posix = std.posix;
const Allocator = std.mem.Allocator;
// Compression support (simplified for now)

const mmap_chunk = @import("mmap_chunk.zig");
const MmapChunk = mmap_chunk.MmapChunk;
pub const DataPoint = mmap_chunk.DataPoint;

// Cold storage for compressed chunk archives.
// Stores multiple chunks in a compressed file format.
//
// Archive format:
//   Header (32 bytes):
//     - magic: u32 = 0x5A544341 ("ZTCA" - ZT Cold Archive)
//     - version: u32 = 1
//     - chunk_count: u32
//     - compression: u32 (0=none, 1=zlib)
//     - created_ts: i64
//     - reserved: 8 bytes
//   Index (chunk_count * 32 bytes each):
//     - series_id: u64
//     - offset: u64 (from start of data section)
//     - compressed_size: u32
//     - uncompressed_size: u32
//     - min_ts: i64
//     - max_ts: i64 (wait this is 40 bytes, need to fix)
//   Data:
//     - compressed chunk data blocks
pub const ColdStorage = struct {
    allocator: Allocator,
    dir: fs.Dir,
    archives: std.StringHashMap(*Archive),

    const MAGIC: u32 = 0x5A544341; // "ZTCA"
    const VERSION: u32 = 1;

    pub const CompressionType = enum(u32) {
        none = 0,
        deflate = 1,
    };

    pub const ArchiveHeader = extern struct {
        magic: u32,
        version: u32,
        chunk_count: u32,
        compression: u32,
        created_ts: i64,
        _reserved: [8]u8,
    };

    pub const ChunkIndex = extern struct {
        series_id: u64,
        min_ts: i64,
        max_ts: i64,
        offset: u64,
        compressed_size: u32,
        uncompressed_size: u32,
    };

    comptime {
        if (@sizeOf(ArchiveHeader) != 32) @compileError("ArchiveHeader size mismatch");
        if (@sizeOf(ChunkIndex) != 40) @compileError("ChunkIndex size mismatch");
    }

    pub const Archive = struct {
        allocator: Allocator,
        path: []const u8,
        file: ?fs.File,
        header: ArchiveHeader,
        index: []ChunkIndex,
        data_offset: u64,

        pub fn open(allocator: Allocator, dir: fs.Dir, path: []const u8) !*Archive {
            const archive = try allocator.create(Archive);
            errdefer allocator.destroy(archive);

            const path_copy = try allocator.dupe(u8, path);
            errdefer allocator.free(path_copy);

            const file = try dir.openFile(path, .{ .mode = .read_only });
            errdefer file.close();

            // Read header
            var header: ArchiveHeader = undefined;
            const header_bytes = std.mem.asBytes(&header);
            const read_len = try file.readAll(header_bytes);
            if (read_len != @sizeOf(ArchiveHeader)) {
                return error.InvalidArchive;
            }

            if (header.magic != MAGIC) return error.InvalidMagic;
            if (header.version != VERSION) return error.InvalidVersion;

            // Read index
            const index = try allocator.alloc(ChunkIndex, header.chunk_count);
            errdefer allocator.free(index);

            const index_bytes = std.mem.sliceAsBytes(index);
            const idx_read = try file.readAll(index_bytes);
            if (idx_read != index_bytes.len) {
                return error.InvalidArchive;
            }

            archive.* = .{
                .allocator = allocator,
                .path = path_copy,
                .file = file,
                .header = header,
                .index = index,
                .data_offset = @sizeOf(ArchiveHeader) + @as(u64, header.chunk_count) * @sizeOf(ChunkIndex),
            };

            return archive;
        }

        pub fn deinit(self: *Archive) void {
            if (self.file) |file| {
                file.close();
                self.file = null;
            }
            self.allocator.free(self.index);
            self.allocator.free(self.path);
            self.allocator.destroy(self);
        }

        // Find chunks that may contain data for series in time range
        pub fn findChunks(self: *const Archive, series_id: u64, start_ts: i64, end_ts: i64) []const ChunkIndex {
            var start_idx: usize = 0;
            var end_idx: usize = 0;
            var found = false;

            for (self.index, 0..) |entry, i| {
                if (entry.series_id == series_id) {
                    // Check time overlap
                    if (!(end_ts < entry.min_ts or start_ts > entry.max_ts)) {
                        if (!found) {
                            start_idx = i;
                            found = true;
                        }
                        end_idx = i + 1;
                    }
                }
            }

            if (!found) return &.{};
            return self.index[start_idx..end_idx];
        }

        // Read and decompress a chunk's data
        pub fn readChunk(self: *Archive, entry: *const ChunkIndex) !ChunkData {
            const file = self.file orelse return error.FileClosed;

            // Seek to chunk data
            try file.seekTo(self.data_offset + entry.offset);

            // Read compressed data
            const compressed = try self.allocator.alloc(u8, entry.compressed_size);
            defer self.allocator.free(compressed);

            const read_len = try file.readAll(compressed);
            if (read_len != entry.compressed_size) {
                return error.ReadError;
            }

            // Decompress
            const compression: CompressionType = @enumFromInt(self.header.compression);
            const decompressed = switch (compression) {
                .none => try self.allocator.dupe(u8, compressed),
                .deflate => try decompressDeflate(self.allocator, compressed, entry.uncompressed_size),
            };

            return ChunkData.fromBytes(self.allocator, entry.series_id, decompressed);
        }
    };

    pub const ChunkData = struct {
        allocator: Allocator,
        series_id: u64,
        timestamps: []const i64,
        values: []const f64,
        data_buf: []u8,

        pub fn fromBytes(allocator: Allocator, series_id: u64, data: []u8) !ChunkData {
            // Data format: count:u64, timestamps:[]i64, values:[]f64
            if (data.len < 8) return error.InvalidData;

            const count: usize = @intCast(std.mem.readInt(u64, data[0..8], .little));
            const expected_size = 8 + count * @sizeOf(i64) + count * @sizeOf(f64);

            if (data.len < expected_size) return error.InvalidData;

            const ts_start = 8;
            const ts_end = ts_start + count * @sizeOf(i64);
            const val_start = ts_end;

            // Need to copy because alignment may not be correct
            const timestamps = try allocator.alloc(i64, count);
            errdefer allocator.free(timestamps);
            const values = try allocator.alloc(f64, count);
            errdefer allocator.free(values);

            @memcpy(std.mem.sliceAsBytes(timestamps), data[ts_start..ts_end]);
            @memcpy(std.mem.sliceAsBytes(values), data[val_start..]);

            return ChunkData{
                .allocator = allocator,
                .series_id = series_id,
                .timestamps = timestamps,
                .values = values,
                .data_buf = data,
            };
        }

        pub fn deinit(self: *ChunkData) void {
            self.allocator.free(self.timestamps);
            self.allocator.free(self.values);
            self.allocator.free(self.data_buf);
        }

        pub fn query(self: *const ChunkData, allocator: Allocator, start_ts: i64, end_ts: i64, result: *std.ArrayList(DataPoint)) !void {
            for (self.timestamps, self.values) |ts, val| {
                if (ts >= start_ts and ts <= end_ts) {
                    try result.append(allocator, .{
                        .series_id = self.series_id,
                        .timestamp = ts,
                        .value = val,
                    });
                }
            }
        }
    };

    pub fn init(allocator: Allocator, dir: fs.Dir) ColdStorage {
        return ColdStorage{
            .allocator = allocator,
            .dir = dir,
            .archives = std.StringHashMap(*Archive).init(allocator),
        };
    }

    pub fn deinit(self: *ColdStorage) void {
        // Free all archives and their keys
        var it = self.archives.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.archives.deinit();
    }

    // Create a new archive from multiple mmap'd chunks
    pub fn createArchive(
        self: *ColdStorage,
        path: []const u8,
        chunks: []const ChunkInput,
        compression: CompressionType,
    ) !void {
        const file = try self.dir.createFile(path, .{});
        defer file.close();

        // Build index and compress chunks
        var index = try self.allocator.alloc(ChunkIndex, chunks.len);
        defer self.allocator.free(index);

        var data_blocks = std.ArrayList([]u8){};
        defer {
            for (data_blocks.items) |block| {
                self.allocator.free(block);
            }
            data_blocks.deinit(self.allocator);
        }

        var current_offset: u64 = 0;

        for (chunks, 0..) |chunk, i| {
            // Serialize chunk data
            const data = try self.serializeChunk(chunk);
            defer self.allocator.free(data);

            // Compress
            const compressed = switch (compression) {
                .none => try self.allocator.dupe(u8, data),
                .deflate => try compressDeflate(self.allocator, data),
            };

            index[i] = .{
                .series_id = chunk.series_id,
                .min_ts = chunk.min_ts,
                .max_ts = chunk.max_ts,
                .offset = current_offset,
                .compressed_size = @intCast(compressed.len),
                .uncompressed_size = @intCast(data.len),
            };

            current_offset += compressed.len;
            try data_blocks.append(self.allocator, compressed);
        }

        // Write header
        const header = ArchiveHeader{
            .magic = MAGIC,
            .version = VERSION,
            .chunk_count = @intCast(chunks.len),
            .compression = @intFromEnum(compression),
            .created_ts = std.time.timestamp(),
            ._reserved = [_]u8{0} ** 8,
        };
        try file.writeAll(std.mem.asBytes(&header));

        // Write index
        try file.writeAll(std.mem.sliceAsBytes(index));

        // Write data blocks
        for (data_blocks.items) |block| {
            try file.writeAll(block);
        }
    }

    pub const ChunkInput = struct {
        series_id: u64,
        min_ts: i64,
        max_ts: i64,
        timestamps: []const i64,
        values: []const f64,
    };

    fn serializeChunk(self: *ColdStorage, chunk: ChunkInput) ![]u8 {
        const count = chunk.timestamps.len;
        const size = 8 + count * @sizeOf(i64) + count * @sizeOf(f64);

        const data = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(data);

        // Write count
        std.mem.writeInt(u64, data[0..8], @intCast(count), .little);

        // Write timestamps and values
        const ts_start = 8;
        const ts_end = ts_start + count * @sizeOf(i64);
        @memcpy(data[ts_start..ts_end], std.mem.sliceAsBytes(chunk.timestamps));
        @memcpy(data[ts_end..], std.mem.sliceAsBytes(chunk.values));

        return data;
    }

    // Open an archive
    pub fn openArchive(self: *ColdStorage, path: []const u8) !*Archive {
        if (self.archives.get(path)) |archive| {
            return archive;
        }

        const archive = try Archive.open(self.allocator, self.dir, path);
        try self.archives.put(try self.allocator.dupe(u8, path), archive);
        return archive;
    }

    // Query data from cold storage
    pub fn query(
        self: *ColdStorage,
        series_id: u64,
        start_ts: i64,
        end_ts: i64,
        result: *std.ArrayList(DataPoint),
    ) !void {
        var it = self.archives.valueIterator();
        while (it.next()) |archive| {
            const chunks = archive.*.findChunks(series_id, start_ts, end_ts);
            for (chunks) |*entry| {
                var chunk_data = try archive.*.readChunk(entry);
                defer chunk_data.deinit();
                try chunk_data.query(self.allocator, start_ts, end_ts, result);
            }
        }
    }
};

// Simple compression placeholder - just copies data
// TODO: Implement proper compression with zstd or deflate
fn compressDeflate(allocator: Allocator, data: []const u8) ![]u8 {
    return try allocator.dupe(u8, data);
}

fn decompressDeflate(allocator: Allocator, data: []const u8, _: u32) ![]u8 {
    return try allocator.dupe(u8, data);
}

// Tests

test "ColdStorage create and query archive" {
    const allocator = testing.allocator;
    const dir = fs.cwd();
    const path = "/tmp/test_cold_archive.zta";

    var storage = ColdStorage.init(allocator, dir);
    defer storage.deinit();

    // Create test chunks
    const ts1 = [_]i64{ 1000, 2000, 3000 };
    const val1 = [_]f64{ 1.1, 2.2, 3.3 };
    const ts2 = [_]i64{ 4000, 5000, 6000 };
    const val2 = [_]f64{ 4.4, 5.5, 6.6 };

    const chunks = [_]ColdStorage.ChunkInput{
        .{ .series_id = 1, .min_ts = 1000, .max_ts = 3000, .timestamps = &ts1, .values = &val1 },
        .{ .series_id = 1, .min_ts = 4000, .max_ts = 6000, .timestamps = &ts2, .values = &val2 },
    };

    // Create archive with zlib compression
    try storage.createArchive(path, &chunks, .deflate);
    defer dir.deleteFile(path) catch {};

    // Open and query
    _ = try storage.openArchive(path);

    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try storage.query(1, 0, 10000, &result);
    try testing.expectEqual(@as(usize, 6), result.items.len);
}

test "ColdStorage uncompressed" {
    const allocator = testing.allocator;
    const dir = fs.cwd();
    const path = "/tmp/test_cold_uncompressed.zta";

    var storage = ColdStorage.init(allocator, dir);
    defer storage.deinit();

    const ts = [_]i64{ 100, 200, 300 };
    const val = [_]f64{ 10, 20, 30 };

    const chunks = [_]ColdStorage.ChunkInput{
        .{ .series_id = 42, .min_ts = 100, .max_ts = 300, .timestamps = &ts, .values = &val },
    };

    try storage.createArchive(path, &chunks, .none);
    defer dir.deleteFile(path) catch {};

    _ = try storage.openArchive(path);

    var result = std.ArrayList(DataPoint){};
    defer result.deinit(allocator);

    try storage.query(42, 150, 250, &result);
    try testing.expectEqual(@as(usize, 1), result.items.len);
    try testing.expectEqual(@as(i64, 200), result.items[0].timestamp);
}
