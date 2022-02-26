#ifndef _SHIM_H
#define _SHIM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include "numa_config.h"
#include "list.h"
#include "rwlock.h"
#include "seqlock.h"

#define MPTABLE_POOL_SIZE_PER_NODE      (2 * 1024 * 1024 * 1024ul)      // 2GB

struct mptable_pool;

struct shim_layer {
    struct mptable_pool *pools[NUM_SOCKET];
};

int shim_layer_init(struct shim_layer *s_layer);
int shim_upsert(int node, pkey_t key, pval_t *val);
int shim_remove(int node, pkey_t key);
int shim_lookup(int node, pkey_t key, pval_t *val);
void shim_dump(int node);

#ifdef __cplusplus
}
#endif

#endif
