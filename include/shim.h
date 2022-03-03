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
