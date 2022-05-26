#ifndef NUMA_CONFIG_H
#define NUMA_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

#define NUM_CPU                 12
#define NUM_DIMM                12
#define NUM_SOCKET              2
#define NUM_CPU_PER_SOCKET      (NUM_CPU / NUM_SOCKET)
#define NUM_DIMM_PER_SOCKET     (NUM_DIMM / NUM_SOCKET)

static inline int node_idx_to_cpu(int node, int cpu_idx) {
    return NUM_SOCKET * cpu_idx + node;
}

static inline int node_idx_to_dimm(int node, int dimm_idx) {
    return dimm_idx + NUM_DIMM_PER_SOCKET * node;
}

static inline int cpu_to_node(int cpu) {
    return cpu % NUM_SOCKET;
}

static inline int dimm_to_node(int dimm) {
    return dimm / NUM_DIMM_PER_SOCKET;
}

static inline int cpu_to_idx(int cpu) {
    return cpu / NUM_SOCKET;
}

static inline int dimm_to_idx(int dimm) {
    return dimm % NUM_DIMM_PER_SOCKET;
}

#ifdef __cplusplus
}
#endif

#endif
