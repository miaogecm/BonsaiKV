#ifndef _SHIM_H
#define _SHIM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "kfifo.h"
#include "data_layer.h"

#define INODE_SIZE								sizeof(inode_t)
#define CPU_TOTAL_INODE                         (CPU_INODE_POOL_SIZE / INODE_SIZE)

#define SMO_LOG_QUEUE_CAPACITY_PER_THREAD       2048

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, const void *key, size_t len, const void* value);
typedef int (*update_func_t)(void* index_struct, const void *key, size_t len, const void* value);
typedef int (*remove_func_t)(void* index_struct, const void *key, size_t len);
typedef void* (*lookup_func_t)(void* index_struct, const void *key, size_t len, const void *actual_key);
typedef int (*scan_func_t)(void* index_struct, const void *low, const void *high);

struct index_layer {
	void *index_struct;

    insert_func_t insert;
    update_func_t update;
	remove_func_t remove;
	lookup_func_t lookup;
	scan_func_t   scan;

	destory_func_t destory;
};

struct inode;

struct smo_log {
    unsigned long ts;
    pkey_t k;
    void *v;
};

struct inode_pool {
    void *start;
    struct inode *freelist;
} ____cacheline_aligned2;

struct shim_layer {
    atomic_t exit;

    DECLARE_KFIFO(fifo, struct smo_log, SMO_LOG_QUEUE_CAPACITY_PER_THREAD)[NUM_CPU];

    struct inode      *head;
    struct inode_pool *pool;
};

struct log_info {
    struct oplog *oplog;
    unsigned pos;
};

int shim_sentinel_init(pnoid_t sentinel_pnoid);
int shim_upsert(log_state_t *lst, pkey_t key, logid_t log);
int shim_lookup(pkey_t key, pval_t *val);
int shim_scan(pkey_t start, int range, pval_t *values);
int shim_sync(log_state_t *lst, pnoid_t start, pnoid_t end, void *rec);
pnoid_t shim_pnode_of(pkey_t key);

void *shim_create_recycle_chain();
void shim_recycle(void *rec);

void index_layer_init(char* index_name, struct index_layer* layer, init_func_t init,
                      insert_func_t insert, update_func_t update, remove_func_t remove,
				      lookup_func_t lookup, scan_func_t scan, destory_func_t destroy);
void index_layer_deinit(struct index_layer* layer);

#ifdef __cplusplus
}
#endif

#endif
