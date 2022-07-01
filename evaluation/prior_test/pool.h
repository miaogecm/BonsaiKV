/*
 * Intel PMEM Performance Test
 */

#ifndef NVM_PERF_POOL_H
#define NVM_PERF_POOL_H

#include <stdlib.h>

void *nvm_create_pool(int id, size_t size);

#endif //NVM_PERF_POOL_H
