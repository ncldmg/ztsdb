# BPF program build Makefile
#
# Prerequisites:
#   - clang (with BPF target support)
#   - libbpf-dev (for headers)
#   - linux-headers (for kernel types)
#
# Install on Arch Linux:
#   sudo pacman -S clang libbpf linux-headers
#
# Install on Ubuntu/Debian:
#   sudo apt install clang libbpf-dev linux-headers-$(uname -r)

CLANG ?= clang
LLVM_STRIP ?= llvm-strip
BPFTOOL ?= bpftool

# Architecture detection
ARCH := $(shell uname -m)
ifeq ($(ARCH),x86_64)
    TARGET_ARCH := x86
    BPF_TARGET := bpfel
else ifeq ($(ARCH),aarch64)
    TARGET_ARCH := arm64
    BPF_TARGET := bpfel
else
    $(error Unsupported architecture: $(ARCH))
endif

# Include paths for BPF headers
# Try common locations
LIBBPF_HEADERS := $(shell pkg-config --cflags libbpf 2>/dev/null || echo "-I/usr/include")
KERNEL_HEADERS := -I/usr/include

# Clang flags for BPF compilation
CFLAGS := -O2 -g \
    -target bpf \
    -D__TARGET_ARCH_$(TARGET_ARCH) \
    $(LIBBPF_HEADERS) \
    $(KERNEL_HEADERS) \
    -Wall -Werror

# Source and output files
SRC := cgroupmon.bpf.c
OUT_X86 := cgroupmon_x86_bpfel.o
OUT_ARM64 := cgroupmon_arm64_bpfel.o

.PHONY: all clean x86 arm64 both

# Default: build for current architecture
all: $(ARCH)

x86_64: x86
aarch64: arm64

# Build for x86_64
x86: $(OUT_X86)
$(OUT_X86): $(SRC)
	$(CLANG) $(CFLAGS) -D__TARGET_ARCH_x86 -c $< -o $@
	@echo "Built $@"

# Build for ARM64
arm64: $(OUT_ARM64)
$(OUT_ARM64): $(SRC)
	$(CLANG) $(CFLAGS) -D__TARGET_ARCH_arm64 -c $< -o $@
	@echo "Built $@"

# Build for both architectures (for distribution)
both: x86 arm64

# Generate vmlinux.h from BTF (optional, for CO-RE)
vmlinux.h:
	$(BPFTOOL) btf dump file /sys/kernel/btf/vmlinux format c > $@

# Strip debug info for smaller binaries (optional)
strip: all
	$(LLVM_STRIP) -g $(OUT_X86) 2>/dev/null || true
	$(LLVM_STRIP) -g $(OUT_ARM64) 2>/dev/null || true

# Show BPF object info
info:
	@echo "=== x86 Object ==="
	@llvm-objdump -h $(OUT_X86) 2>/dev/null || echo "Not built"
	@echo ""
	@echo "=== ARM64 Object ==="
	@llvm-objdump -h $(OUT_ARM64) 2>/dev/null || echo "Not built"

clean:
	rm -f $(OUT_X86) $(OUT_ARM64) vmlinux.h

# Help
help:
	@echo "BPF Program Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all      - Build for current architecture (default)"
	@echo "  x86      - Build for x86_64"
	@echo "  arm64    - Build for ARM64"
	@echo "  both     - Build for both architectures"
	@echo "  strip    - Strip debug info from objects"
	@echo "  info     - Show object file sections"
	@echo "  clean    - Remove built files"
	@echo ""
	@echo "Prerequisites:"
	@echo "  clang, libbpf-dev, linux-headers"
