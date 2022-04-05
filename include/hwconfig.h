#ifndef NUMA_CONFIG_H
#define NUMA_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

#define NUM_CPU                 12
#define NUM_DIMM                6
#define NUM_SOCKET              2
#define NUM_CPU_PER_SOCKET      (NUM_CPU / NUM_SOCKET)
#define NUM_DIMM_PER_SOCKET     (NUM_DIMM / NUM_SOCKET)

static inline int node_to_cpu(int node, int cpu_idx) {
    return NUM_SOCKET * cpu_idx + node;
}

static inline int node_to_dimm(int node, int dimm_idx) {
    return dimm_idx + NUM_DIMM * node;
}

static inline int cpu_to_node(int cpu) {
    return cpu % NUM_SOCKET;
}

#ifdef __cplusplus
}
#endif

#endif
