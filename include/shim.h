#ifndef _SHIM_H
#define _SHIM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "kfifo.h"
#include "pnode.h"

#define SNODE_POOL_SIZE                         (2 * 1024 * 1024 * 1024ul)

#define SMO_LOG_QUEUE_CAPACITY_PER_THREAD       2048

typedef struct {
    pnoid_t pno;
    pkey_t  pfence;
} shim_sync_pfence_t;

struct snode;

struct smo_log {
    unsigned long ts;
    pkey_t k;
    void *v;
};

struct snode_pool {
    void *start;
    struct snode *freelist;
};

struct shim_layer {
    atomic_t exit;

    DECLARE_KFIFO(fifo, struct smo_log, SMO_LOG_QUEUE_CAPACITY_PER_THREAD)[NUM_CPU];

    struct snode      *head;
    struct snode_pool *pool;
};

int shim_layer_init(struct shim_layer *layer);
int shim_sentinel_init(struct shim_layer *layer, pnoid_t sentinel_pnoid);
int shim_upsert(log_state_t *lst, pkey_t key, logid_t log);
int shim_lookup(pkey_t key, pval_t *val);
int shim_sync(log_state_t *lst, shim_sync_pfence_t *pfences);
pnoid_t shim_pnode_of(pkey_t key);

#ifdef __cplusplus
}
#endif

#endif
