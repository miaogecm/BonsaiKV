#ifndef NUMA_CONFIG_H
#define NUMA_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

#define NUM_CPU                 48
#define NUM_DIMM                12
#define NUM_SOCKET              2
#define NUM_CPU_PER_SOCKET      (NUM_CPU / NUM_SOCKET)
#define NUM_DIMM_PER_SOCKET     (NUM_DIMM / NUM_SOCKET)

#define LOG_REGION_SIZE		    73728000000UL                   /* 68.66455078125GB */
#define DATA_REGION_SIZE	    5529600000UL                    /* 5.14984130859375GB */

#define INODE_POOL_SIZE         (16 * 1024 * 1024 * 1024ul)     /* 2147483648 entries */

#define ENABLE_PNODE_REPLICA
#define REPLICA_EPOCH_INTERVAL  1                               /* seconds */

//#define ASYNC_SMO

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
