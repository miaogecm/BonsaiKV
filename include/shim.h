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

#define MPTABLE_POOL_SIZE      (2 * 1024 * 1024 * 1024ul)      // 2GB

struct mptable_pool;

#define SMO_CHKPT				        100

struct smo_log {
    int node;
    int len;
    pkey_t k;
    pval_t v;
};

struct smo_list {
    spinlock_t lock;
	struct smo_log smo[SMO_CHKPT];
	int num_used;
    struct smo_list* next;
};

struct shim_layer {
    atomic_t exit;
	atomic_t force_smo;
	struct smo_list *st_smo_log, *curr_smo_log;

    struct mptable_pool *pools;
};

int shim_layer_init(struct shim_layer *s_layer);
int shim_upsert(pkey_t key, pval_t *val);
int shim_remove(pkey_t key);
int shim_lookup(pkey_t key, pval_t *val);
int shim_update(pkey_t key, const pval_t *old_val, pval_t *new_val);
void shim_dump();

#ifdef __cplusplus
}
#endif

#endif
