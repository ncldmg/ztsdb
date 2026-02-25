.PHONY: build test clean run serve web generate fmt check bench bpf

# Build the project
build:
	zig build

# Build BPF programs (requires clang and libbpf-dev)
bpf:
	$(MAKE) -C src/bpf all

# Build BPF for both architectures
bpf-all:
	$(MAKE) -C src/bpf both

# Build with web assets
all: build web

# Run all tests
test:
	zig build test --summary all

# Run tests with verbose output
test-verbose:
	zig build test --summary all 2>&1 | cat

# Clean build artifacts
clean:
	rm -rf .zig-cache zig-out tmp/*.wal

# Run the server
serve:
	@mkdir -p tmp
	zig build && ./zig-out/bin/tsvdb serve --port 9876

# Run the server with web UI
run:
	@mkdir -p tmp
	zig build && zig build web && ./zig-out/bin/tsvdb serve --port 9876 --web-port 8080

# Build web assets
web:
	zig build web

# Generate test data (100 points)
generate:
	./zig-out/bin/tsvdb generate --port 9876 --series 1 --count 100

# Generate more test data (1000 points)
generate-large:
	./zig-out/bin/tsvdb generate --port 9876 --series 1 --count 1000 --interval 100

# Generate data for multiple series
generate-multi:
	./zig-out/bin/tsvdb generate --port 9876 --series 1 --count 100
	./zig-out/bin/tsvdb generate --port 9876 --series 2 --count 100
	./zig-out/bin/tsvdb generate --port 9876 --series 3 --count 100

# Format source files
fmt:
	zig fmt src/

# Check formatting without modifying
check:
	zig fmt --check src/

# Build in release mode
release:
	zig build -Doptimize=ReleaseFast

# Run specific test file
test-server:
	zig build test --summary all 2>&1 | grep -E "(server|passed|failed)"

test-client:
	zig build test --summary all 2>&1 | grep -E "(client|passed|failed)"

test-protocol:
	zig build test --summary all 2>&1 | grep -E "(protocol|passed|failed)"

test-tsdb:
	zig build test --summary all 2>&1 | grep -E "(tsdb|passed|failed)"

# Run benchmarks
bench:
	zig build bench
