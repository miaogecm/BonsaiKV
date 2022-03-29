/*
 * Intel PMEM Performance Test
 */

#ifndef NVM_INTERLEAVE_GRANULARITY_CONFIG_H
#define NVM_INTERLEAVE_GRANULARITY_CONFIG_H

#define CONFIG_NUMA_NODE        0
#define CONFIG_DIMM_CNT         6

#define CONFIG_SIZE             (25769803776ul)

static size_t chunk_sizes[] = { 256, 512, 1024, 2048, 4096 };
static int num_workers[] = { 1, 2, 4, 8, 16, 24 };

#endif //NVM_INTERLEAVE_GRANULARITY_CONFIG_H
