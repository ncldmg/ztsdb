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
    pending_writes: usize, // Track queued but not completed writes

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
            .pending_writes = 0,
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

    // Queue write to io_uring (does not submit). Call submit() or flush() after.
    pub fn write(self: *WAL, data: []const u8) !u64 {
        if (self.write_offset + data.len > self.max_segment_size) {
            return error.WALFull;
        }

        const offset = self.write_offset;

        // Queue write operation to io_uring
        const sqe = self.io_uring.write(
            @intFromPtr(data.ptr),
            self.file.handle,
            data,
            offset,
        ) catch |err| {
            std.log.err("WAL write queue failed: {}", .{err});
            return error.WriteError;
        };
        sqe.user_data = offset; // Tag with offset for debugging

        self.write_offset += data.len;
        self.pending_writes += 1;
        return offset;
    }

    // Submit all queued writes to kernel (non-blocking, doesn't wait for completion)
    pub fn submit(self: *WAL) !usize {
        if (self.pending_writes == 0) return 0;

        const submitted = self.io_uring.submit() catch |err| {
            std.log.err("WAL submit failed: {}", .{err});
            return error.WriteError;
        };

        return submitted;
    }

    // Queue multiple writes (does not submit). Call submit() or flush() after.
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

        // Queue all writes to io_uring without waiting
        for (entries, 0..) |entry, i| {
            offsets[i] = self.write_offset;

            const sqe = self.io_uring.write(
                @intFromPtr(entry.ptr),
                self.file.handle,
                entry,
                self.write_offset,
            ) catch |err| {
                std.log.err("WAL batch write queue failed at entry {}: {}", .{ i, err });
                return error.WriteError;
            };
            sqe.user_data = offsets[i];

            self.write_offset += entry.len;
            self.pending_writes += 1;
        }

        return offsets;
    }

    // Submit and wait for all pending writes to complete
    pub fn flush(self: *WAL) !void {
        if (self.pending_writes == 0) {
            return; // Nothing to flush
        }

        // Submit any pending operations
        const submitted = self.io_uring.submit() catch |err| {
            std.log.err("WAL flush submit failed: {}", .{err});
            return error.WriteError;
        };

        if (submitted == 0 and self.pending_writes > 0) {
            std.log.warn("WAL flush: {} pending but 0 submitted", .{self.pending_writes});
        }

        // Wait for all pending writes to complete
        var cqes: [QUEUE_DEPTH]linux.io_uring_cqe = undefined;
        var remaining = self.pending_writes;

        while (remaining > 0) {
            const completed = self.io_uring.copy_cqes(&cqes, 1) catch |err| {
                std.log.err("WAL flush completion failed: {}", .{err});
                return error.WriteError;
            };

            if (completed == 0) {
                continue; // Keep waiting
            }

            // Check for errors in completed operations
            for (cqes[0..completed]) |cqe| {
                if (cqe.err() != .SUCCESS) {
                    std.log.err("WAL flush io_uring error: {}", .{cqe.err()});
                    return error.WriteError;
                }
            }

            remaining -= completed;
        }

        self.pending_writes = 0;
    }

    // Force fsync to ensure durability (required for crash consistency)
    pub fn sync(self: *WAL) !void {
        // First flush any pending writes
        try self.flush();

        // Queue fsync operation
        const sqe = self.io_uring.fsync(0, self.file.handle, 0) catch |err| {
            std.log.err("fsync queue failed: {}", .{err});
            return error.SyncError;
        };
        sqe.user_data = 0; // Mark as fsync operation

        // Submit and wait for fsync
        const submitted = self.io_uring.submit() catch |err| {
            std.log.err("fsync submit failed: {}", .{err});
            return error.SyncError;
        };
        if (submitted == 0) {
            std.log.err("fsync: no operations submitted", .{});
            return error.SyncError;
        }

        // Wait for completion
        var cqes: [1]linux.io_uring_cqe = undefined;
        while (true) {
            const completed = self.io_uring.copy_cqes(&cqes, 1) catch |err| {
                std.log.err("fsync completion failed: {}", .{err});
                return error.SyncError;
            };

            if (completed > 0) {
                if (cqes[0].err() != .SUCCESS) {
                    std.log.err("fsync io_uring error: {}", .{cqes[0].err()});
                    return error.SyncError;
                }
                break;
            }
        }
    }

    // Read data from WAL at specific offset
    pub fn read(self: *WAL, offset: u64, buffer: []u8) !usize {
        if (offset >= self.write_offset) {
            return error.InvalidOffset;
        }

        const bytes_to_read = @min(buffer.len, self.write_offset - offset);
        const read_buffer = buffer[0..bytes_to_read];

        // Queue read operation
        const sqe = self.io_uring.read(
            @intFromPtr(read_buffer.ptr),
            self.file.handle,
            read_buffer,
            offset,
        ) catch |err| {
            std.log.err("WAL read queue failed: {}", .{err});
            return error.ReadError;
        };
        sqe.user_data = offset;

        // Submit the read
        const submitted = self.io_uring.submit() catch |err| {
            std.log.err("WAL read submit failed: {}", .{err});
            return error.ReadError;
        };
        if (submitted == 0) {
            std.log.err("WAL read: no operations submitted", .{});
            return error.ReadError;
        }

        // Wait for completion
        var cqes: [1]linux.io_uring_cqe = undefined;
        const completed = self.io_uring.copy_cqes(&cqes, 1) catch |err| {
            std.log.err("WAL read completion failed: {}", .{err});
            return error.ReadError;
        };

        if (completed == 0) {
            std.log.err("WAL read: no completions received", .{});
            return error.ReadError;
        }

        if (cqes[0].err() != .SUCCESS) {
            std.log.err("WAL read io_uring error: {}", .{cqes[0].err()});
            return error.ReadError;
        }

        return @intCast(cqes[0].res);
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
