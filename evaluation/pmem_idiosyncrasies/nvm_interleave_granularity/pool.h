/*
 * Intel PMEM Performance Test
 */

#ifndef NVM_PERF_POOL_H
#define NVM_PERF_POOL_H

#include <stdlib.h>

static int node_dimm_id(int node, int n) {
    return 2 * node + n;
}

void *nvm_create_pool(int id, size_t size);
void nvm_destroy_pool(void *p, size_t size);

#endif //NVM_PERF_POOL_H
