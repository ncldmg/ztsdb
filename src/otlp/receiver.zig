const std = @import("std");
const tsdb_mod = @import("../timeserie/tsdb.zig");
const registry_mod = @import("../metrics/registry.zig");
const TSDB = tsdb_mod.TSDB;
const DataPoint = tsdb_mod.DataPoint;
const Registry = registry_mod.Registry;
const protobuf = @import("protobuf.zig");

/// OTLP Metrics Receiver
/// Implements OpenTelemetry Protocol HTTP receiver for application metrics
pub const OTLPReceiver = struct {
    allocator: std.mem.Allocator,
    tsdb: *TSDB,
    registry: *Registry,

    pub fn init(allocator: std.mem.Allocator, tsdb: *TSDB, registry: *Registry) OTLPReceiver {
        return .{
            .allocator = allocator,
            .tsdb = tsdb,
            .registry = registry,
        };
    }

    pub fn deinit(self: *OTLPReceiver) void {
        // Registry is owned by caller, nothing to clean up here
        _ = self;
    }

    /// Handle OTLP HTTP POST request
    /// Parses Protobuf or JSON payload and stores metrics in TSDB
    pub fn handleMetrics(self: *OTLPReceiver, body: []const u8, is_protobuf: bool) ![]const u8 {
        if (is_protobuf) {
            return try self.handleProtobufMetrics(body);
        } else {
            return try self.handleJsonMetrics(body);
        }
    }

    /// Handle Protobuf-encoded OTLP metrics
    fn handleProtobufMetrics(self: *OTLPReceiver, body: []const u8) ![]const u8 {
        std.log.info("Parsing Protobuf OTLP metrics ({d} bytes)", .{body.len});

        // Parse the protobuf data
        const metric_points = try protobuf.parseOTLPMetrics(self.allocator, body);
        defer self.allocator.free(metric_points);

        // Store each metric point in the TSDB
        for (metric_points) |point| {
            const series_name = if (point.container_name) |name|
                try std.fmt.allocPrint(
                    self.allocator,
                    "{s}.{s}",
                    .{ point.metric_name, name },
                )
            else
                try self.allocator.dupe(u8, point.metric_name);
            defer self.allocator.free(series_name);

            try self.storeDataPoint(series_name, point.timestamp, point.value);

            std.log.info("OTLP METRIC: {s} = {d:.2} @ {d}", .{
                series_name,
                point.value,
                point.timestamp / 1_000_000_000,
            });
        }

        std.log.info("Processed {d} metric points from Protobuf", .{metric_points.len});
        return "{}"; // Return empty JSON response (OTLP success)
    }

    /// Handle JSON-encoded OTLP metrics
    fn handleJsonMetrics(self: *OTLPReceiver, body: []const u8) ![]const u8 {
        // Debug: Log the incoming payload
        std.log.debug("Received payload ({d} bytes): {s}", .{ body.len, body[0..@min(body.len, 200)] });

        // Parse JSON body
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            body,
            .{},
        ) catch |err| {
            std.log.err("JSON parse error: {}", .{err});
            std.log.err("Payload: {s}", .{body});
            return error.SyntaxError;
        };
        defer parsed.deinit();

        const root = parsed.value;

        // OTLP JSON structure:
        // {
        //   "resourceMetrics": [{
        //     "resource": { "attributes": [...] },
        //     "scopeMetrics": [{
        //       "metrics": [{
        //         "name": "container.cpu.usage",
        //         "gauge": { "dataPoints": [{ "asDouble": 42.5, ... }] }
        //       }]
        //     }]
        //   }]
        // }

        if (root.object.get("resourceMetrics")) |resource_metrics_value| {
            const resource_metrics = resource_metrics_value.array;

            for (resource_metrics.items) |resource_metric| {
                try self.processResourceMetric(resource_metric);
            }
        }

        return "{}"; // Return empty JSON response (OTLP success)
    }

    fn processResourceMetric(self: *OTLPReceiver, resource_metric: std.json.Value) !void {
        // Extract resource attributes (container metadata)
        var container_id: ?[]const u8 = null;
        var container_name: ?[]const u8 = null;

        if (resource_metric.object.get("resource")) |resource| {
            if (resource.object.get("attributes")) |attrs| {
                for (attrs.array.items) |attr| {
                    const key = attr.object.get("key").?.string;
                    const value = attr.object.get("value").?.object.get("stringValue").?.string;

                    if (std.mem.eql(u8, key, "container.id")) {
                        container_id = value;
                    } else if (std.mem.eql(u8, key, "container.name")) {
                        container_name = value;
                    }
                }
            }
        }

        // Process scope metrics
        if (resource_metric.object.get("scopeMetrics")) |scope_metrics_value| {
            for (scope_metrics_value.array.items) |scope_metric| {
                if (scope_metric.object.get("metrics")) |metrics| {
                    for (metrics.array.items) |metric| {
                        try self.processMetric(metric, container_id, container_name);
                    }
                }
            }
        }
    }

    fn processMetric(
        self: *OTLPReceiver,
        metric: std.json.Value,
        container_id: ?[]const u8,
        container_name: ?[]const u8,
    ) !void {
        _ = container_id; // TODO: Use for series tagging
        const metric_name = metric.object.get("name").?.string;

        // Construct time series name from metric name and container
        const series_name = if (container_name) |name|
            try std.fmt.allocPrint(
                self.allocator,
                "{s}.{s}",
                .{ metric_name, name },
            )
        else
            try self.allocator.dupe(u8, metric_name);
        defer self.allocator.free(series_name);

        // Extract data points from gauge or counter
        var data_points: ?std.json.Value = null;

        if (metric.object.get("gauge")) |gauge| {
            data_points = gauge.object.get("dataPoints");
        } else if (metric.object.get("sum")) |sum| {
            data_points = sum.object.get("dataPoints");
        }

        if (data_points) |points| {
            for (points.array.items) |point| {
                const timestamp_ns = point.object.get("timeUnixNano").?.string;
                const timestamp = try std.fmt.parseInt(u64, timestamp_ns, 10);

                var value: f64 = 0.0;
                if (point.object.get("asDouble")) |v| {
                    value = v.float;
                } else if (point.object.get("asInt")) |v| {
                    value = @floatFromInt(try std.fmt.parseInt(i64, v.string, 10));
                }

                try self.storeDataPoint(series_name, timestamp, value);

                std.debug.print(
                    "Stored: {s} = {d} @ {d}\n",
                    .{ series_name, value, timestamp },
                );
            }
        }
    }

    fn storeDataPoint(self: *OTLPReceiver, series_name: []const u8, timestamp: u64, value: f64) !void {
        // Get or create series ID via shared registry
        const series_id = try self.registry.getOrCreateId(series_name, .otlp);

        // Convert timestamp from nanoseconds to milliseconds
        const timestamp_ms: i64 = @intCast(@divTrunc(timestamp, 1_000_000));

        // Insert into TSDB
        if (!self.tsdb.insert(series_id, timestamp_ms, value)) {
            std.log.err("Failed to insert metric: buffer full", .{});
            return error.BufferFull;
        }

        std.log.debug("STORED: {s} (ID={d}) = {d:.2} @ {d}", .{ series_name, series_id, value, timestamp_ms });
    }

    /// Query metrics by series name
    pub fn queryMetrics(self: *OTLPReceiver, series_name: []const u8, start_ts: i64, end_ts: i64) ![]const u8 {
        // Check if series exists
        const series_id = self.registry.getId(series_name) orelse {
            // Series not found, return empty result
            return try std.fmt.allocPrint(self.allocator, "{{\"series\":\"{s}\",\"points\":[]}}", .{series_name});
        };

        // Query TSDB
        var result: std.ArrayList(DataPoint) = .{};
        defer result.deinit(self.allocator);

        try self.tsdb.query(series_id, start_ts, end_ts, &result);

        // Convert to JSON
        var json: std.ArrayList(u8) = .{};
        defer json.deinit(self.allocator);

        const writer = json.writer(self.allocator);
        try writer.print("{{\"series\":\"{s}\",\"points\":[", .{series_name});

        for (result.items, 0..) |point, i| {
            if (i > 0) try writer.writeAll(",");
            try writer.print("{{\"timestamp\":{d},\"value\":{d}}}", .{ point.timestamp, point.value });
        }

        try writer.writeAll("]}");

        return json.toOwnedSlice(self.allocator);
    }

    /// List all series names (delegates to registry)
    pub fn listSeries(self: *OTLPReceiver) ![]const u8 {
        const json = try self.registry.toJson(self.allocator);
        defer self.allocator.free(json);

        return try std.fmt.allocPrint(self.allocator, "{{\"series\":{s}}}", .{json});
    }
};
