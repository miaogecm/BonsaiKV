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
#define NUM_CPU_PER_DIMM        (NUM_CPU / NUM_DIMM)

#define NUM_USER_THREAD				1
#define NUM_PFLUSH_WORKER_PER_NODE	12

#define LOG_REGION_SIZE		    73728000000UL                   /* 68.66455078125GB */
#define DATA_REGION_SIZE	    55296000000UL                   /* 51.4984130859375GB */

#define CPU_INODE_POOL_SIZE     (512 * 1024 * 1024ul)

//#define ENABLE_PNODE_REPLICA
//#define REPLICA_EPOCH_INTERVAL  1                               /* seconds */

//#define ASYNC_SMO

#define ENABLE_AUTO_CHKPT
#define ENABLE_LOAD_BALANCE

//#define STR_KEY
//#define STR_VAL
#define VAL_LEN                 16384
#define CPU_VAL_POOL_SIZE       384000
#define STAGING_PERSIST_GRANU   256

//#define USE_DEVDAX
#define PMM_PAGE_SIZE           (2 * 1024 * 1024ul)

#define INTERLEAVED_CPU_NR
//#define INTERLEAVED_DIMM_NR

#define DISABLE_OFFLOAD
#define DISABLE_UPLOAD

static inline int node_idx_to_cpu(int node, int cpu_idx) {
#ifdef INTERLEAVED_CPU_NR
    return NUM_SOCKET * cpu_idx + node;
#else
    return cpu_idx + NUM_CPU_PER_SOCKET * node;
#endif
}

static inline int node_idx_to_dimm(int node, int dimm_idx) {
#ifdef INTERLEAVED_DIMM_NR
    return NUM_DIMM * dimm_idx + node;
#else
    return dimm_idx + NUM_DIMM_PER_SOCKET * node;
#endif
}

static inline int cpu_to_node(int cpu) {
#ifdef INTERLEAVED_CPU_NR
    return cpu % NUM_SOCKET;
#else
    return cpu / NUM_CPU_PER_SOCKET;
#endif
}

static inline int dimm_to_node(int dimm) {
#ifdef INTERLEAVED_DIMM_NR
    return dimm % NUM_DIMM;
#else
    return dimm / NUM_DIMM_PER_SOCKET;
#endif
}

static inline int cpu_to_idx(int cpu) {
#ifdef INTERLEAVED_CPU_NR
    return cpu / NUM_SOCKET;
#else
    return cpu % NUM_CPU_PER_SOCKET;
#endif
}

static inline int dimm_to_idx(int dimm) {
#ifdef INTERLEAVED_DIMM_NR
    return dimm / NUM_SOCKET;
#else
    return dimm % NUM_DIMM_PER_SOCKET;
#endif
}

#ifdef __cplusplus
}
#endif

#endif
