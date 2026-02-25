// SPDX-License-Identifier: GPL-2.0
// eBPF program for container metrics collection
//
// Build with:
//   clang -O2 -g -target bpf -D__TARGET_ARCH_x86 -c cgroupmon.bpf.c -o
//   cgroupmon_x86_bpfel.o clang -O2 -g -target bpf -D__TARGET_ARCH_arm64 -c
//   cgroupmon.bpf.c -o cgroupmon_arm64_bpfel.o

#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <linux/bpf.h>
#include <linux/types.h>

char LICENSE[] SEC("license") = "GPL";

// Metrics structure - must match maps.zig CgroupMetrics exactly
struct cgroup_metrics {
  __u64 cpu_time_ns;
  __u64 memory_bytes;
  __u64 net_rx_bytes;
  __u64 net_tx_bytes;
  __u64 net_rx_packets;
  __u64 net_tx_packets;
  __u64 block_read_bytes;
  __u64 block_write_bytes;
  __u64 block_read_ops;
  __u64 block_write_ops;
  __u64 timestamp;
};

// Per-CPU hash map keyed by cgroup ID
struct {
  __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
  __uint(max_entries, 4096);
  __type(key, __u64);
  __type(value, struct cgroup_metrics);
} cgroup_metrics_map SEC(".maps");

// Helper to get or create metrics entry
static __always_inline struct cgroup_metrics *get_metrics(__u64 cgroup_id) {
  struct cgroup_metrics *metrics =
      bpf_map_lookup_elem(&cgroup_metrics_map, &cgroup_id);
  if (!metrics) {
    struct cgroup_metrics zero = {};
    bpf_map_update_elem(&cgroup_metrics_map, &cgroup_id, &zero, BPF_NOEXIST);
    metrics = bpf_map_lookup_elem(&cgroup_metrics_map, &cgroup_id);
  }
  return metrics;
}

// Tracepoint: sched_switch - track CPU time
// Fires on every context switch
SEC("tp/sched/sched_switch")
int handle_sched_switch(void *ctx) {
  __u64 cgroup_id = bpf_get_current_cgroup_id();
  if (cgroup_id == 0)
    return 0;

  struct cgroup_metrics *metrics = get_metrics(cgroup_id);
  if (!metrics)
    return 0;

  // Get CPU time for this task
  __u64 cpu_time = bpf_ktime_get_ns();

  // Update metrics atomically
  __sync_fetch_and_add(&metrics->cpu_time_ns, 1); // Increment on each switch
  metrics->timestamp = cpu_time;

  return 0;
}

// Cgroup SKB: ingress - track incoming network traffic
SEC("cgroup_skb/ingress")
int handle_ingress(struct __sk_buff *skb) {
  __u64 cgroup_id = bpf_skb_cgroup_id(skb);
  if (cgroup_id == 0)
    return 1; // Allow packet

  struct cgroup_metrics *metrics = get_metrics(cgroup_id);
  if (!metrics)
    return 1;

  __sync_fetch_and_add(&metrics->net_rx_bytes, skb->len);
  __sync_fetch_and_add(&metrics->net_rx_packets, 1);
  metrics->timestamp = bpf_ktime_get_ns();

  return 1; // Allow packet
}

// Cgroup SKB: egress - track outgoing network traffic
SEC("cgroup_skb/egress")
int handle_egress(struct __sk_buff *skb) {
  __u64 cgroup_id = bpf_skb_cgroup_id(skb);
  if (cgroup_id == 0)
    return 1; // Allow packet

  struct cgroup_metrics *metrics = get_metrics(cgroup_id);
  if (!metrics)
    return 1;

  __sync_fetch_and_add(&metrics->net_tx_bytes, skb->len);
  __sync_fetch_and_add(&metrics->net_tx_packets, 1);
  metrics->timestamp = bpf_ktime_get_ns();

  return 1; // Allow packet
}

// Tracepoint: sys_enter_mmap - track memory allocations
SEC("tracepoint/syscalls/sys_enter_mmap")
int handle_mmap(void *ctx) {
  __u64 cgroup_id = bpf_get_current_cgroup_id();
  if (cgroup_id == 0)
    return 0;

  struct cgroup_metrics *metrics = get_metrics(cgroup_id);
  if (!metrics)
    return 0;

  // Note: This tracks mmap calls, not actual memory usage
  // For accurate memory, read from cgroup memory.current
  __sync_fetch_and_add(&metrics->memory_bytes, 4096); // Placeholder
  metrics->timestamp = bpf_ktime_get_ns();

  return 0;
}
