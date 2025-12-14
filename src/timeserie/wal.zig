const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const posix = std.posix;
const os = std.os;
const mem = std.mem;
const Allocator = std.mem.Allocator;
const linux = os.linux;

pub const WAL = struct {
    file: fs.File,
    io_uring: linux.IoUring,
    write_offset: u64,
    max_segment_size: u64,
    allocator: Allocator,

    const QUEUE_DEPTH: u13 = 256; // io_uring queue depth for batching

    pub fn init(allocator: Allocator, max_segment_size: u64, dir: fs.Dir, path: []const u8) !WAL {
        const wal_file = try open(dir, path, max_segment_size);
        const ring = try linux.IoUring.init(QUEUE_DEPTH, 0);

        return WAL{
            .file = wal_file,
            .io_uring = ring,
            .write_offset = 0,
            .max_segment_size = max_segment_size,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *WAL) void {
        self.io_uring.deinit();
        self.file.close();
    }

    fn open(dir: fs.Dir, path: []const u8, size: u64) !fs.File {
        const file = try dir.createFile(path, .{
            .read = true,
            .truncate = false,
            .exclusive = false,
        });

        // Pre-allocate file space for better performance
        try file.setEndPos(size);

        return file;
    }

    // Write data to WAL using io_uring. Returns offset where data was written.
    pub fn write(self: *WAL, data: []const u8) !u64 {
        if (self.write_offset + data.len > self.max_segment_size) {
            return error.WALFull;
        }

        const offset = self.write_offset;

        // Submit write operation to io_uring
        _ = try self.io_uring.write(
            @intFromPtr(data.ptr),
            self.file.handle,
            data,
            offset,
        );

        self.write_offset += data.len;
        return offset;
    }

    // Write multiple entries in a batch for maximum throughput
    pub fn writeBatch(self: *WAL, entries: []const []const u8) ![]u64 {
        const offsets = try self.allocator.alloc(u64, entries.len);
        errdefer self.allocator.free(offsets);

        var total_size: u64 = 0;
        for (entries) |entry| {
            total_size += entry.len;
        }

        if (self.write_offset + total_size > self.max_segment_size) {
            return error.WALFull;
        }

        // Submit all writes to io_uring without waiting
        for (entries, 0..) |entry, i| {
            offsets[i] = self.write_offset;

            _ = try self.io_uring.write(
                @intFromPtr(entry.ptr),
                self.file.handle,
                entry,
                self.write_offset,
            );

            self.write_offset += entry.len;
        }

        // Submit all queued operations at once
        _ = try self.io_uring.submit();

        return offsets;
    }

    // Flush all pending writes and wait for completion
    pub fn flush(self: *WAL) !void {
        // Submit any pending operations
        _ = try self.io_uring.submit();

        // Wait for all operations to complete
        var cqes: [QUEUE_DEPTH]linux.io_uring_cqe = undefined;
        const completed = try self.io_uring.copy_cqes(&cqes, 1);

        // Check for errors in completed operations
        for (cqes[0..completed]) |cqe| {
            if (cqe.err() != .SUCCESS) {
                return error.WriteError;
            }
        }
    }

    // Force fsync to ensure durability (required for crash consistency)
    pub fn sync(self: *WAL) !void {
        // First flush any pending writes
        try self.flush();

        // Then fsync to disk
        _ = try self.io_uring.fsync(self.file.handle, 0);
        _ = try self.io_uring.submit();

        var cqe: linux.io_uring_cqe = undefined;
        _ = try self.io_uring.copy_cqes(@as([*]linux.io_uring_cqe, &cqe)[0..1], 1);

        if (cqe.err() != .SUCCESS) {
            return error.SyncError;
        }
    }

    // Read data from WAL at specific offset
    pub fn read(self: *WAL, offset: u64, buffer: []u8) !usize {
        if (offset >= self.write_offset) {
            return error.InvalidOffset;
        }

        const bytes_to_read = @min(buffer.len, self.write_offset - offset);
        const read_buffer = buffer[0..bytes_to_read];

        _ = try self.io_uring.read(
            @intFromPtr(read_buffer.ptr),
            self.file.handle,
            read_buffer,
            offset,
        );

        _ = try self.io_uring.submit();

        var cqe: linux.io_uring_cqe = undefined;
        _ = try self.io_uring.copy_cqes(@as([*]linux.io_uring_cqe, &cqe)[0..1], 1);

        if (cqe.err() != .SUCCESS) {
            return error.ReadError;
        }

        return @intCast(cqe.res);
    }

    // Reset WAL (for testing or when starting new segment)
    pub fn reset(self: *WAL) !void {
        try self.file.seekTo(0);
        self.write_offset = 0;
    }

    // Get current write position
    pub fn getOffset(self: *const WAL) u64 {
        return self.write_offset;
    }

    // Check if WAL is full
    pub fn isFull(self: *const WAL) bool {
        return self.write_offset >= self.max_segment_size;
    }
};
