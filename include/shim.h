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
#include "kfifo.h"
#include "ordo.h"

#define MPTABLE_POOL_SIZE      (2 * 1024 * 1024 * 1024ul)      // 2GB

struct mptable_pool;

#define SMO_LOG_QUEUE_CAPACITY_PER_THREAD       2048

struct smo_log {
    unsigned long ts;
    pkey_t k;
    pval_t v;
};

struct shim_layer {
    atomic_t exit;

    DECLARE_KFIFO(fifo, struct smo_log, SMO_LOG_QUEUE_CAPACITY_PER_THREAD)[NUM_CPU];

    struct mptable_pool *pool;
};

int shim_layer_init(struct shim_layer *s_layer);
int shim_upsert(pkey_t key, pval_t *val);
int shim_remove(pkey_t key);
int shim_lookup(pkey_t key, pval_t *val);
int shim_update(pkey_t key, const pval_t *old_val, pval_t *new_val);
int shim_scan(pkey_t lo, pkey_t hi, pkey_t *arr);
void shim_dump();

#ifdef __cplusplus
}
#endif

#endif
