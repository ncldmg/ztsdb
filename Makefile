.PHONY: build test clean run fmt check

# Build the project
build:
	zig build

# Run all tests
test:
	zig build test --summary all

# Run tests with verbose output
test-verbose:
	zig build test --summary all 2>&1 | cat

# Clean build artifacts
clean:
	rm -rf .zig-cache zig-out

# Run the server (requires --port argument)
run:
	zig build run -- --port 9876

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
