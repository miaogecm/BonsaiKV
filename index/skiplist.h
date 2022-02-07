#ifndef SKIPLIST_H
#define SKIPLIST_H

#include "common.h"
#include "list.h"
#include "spinlock.h"
#include "arch.h"

#define SYNC_SWAP(addr,x)         __sync_lock_test_and_set(addr,x)
#define SYNC_CAS(addr,old,x)      __sync_val_compare_and_swap(addr,old,x)
#define SYNC_ADD(addr,n)          __sync_add_and_fetch(addr,n)
#define SYNC_SUB(addr,n)          __sync_sub_and_fetch(addr,n)

#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))

#ifndef MAX_LEVELS
#define MAX_LEVELS 30
#endif

struct kv_node {
    pentry_t kv;
    int levels;
	int marked;
	int fully_linked;
	spinlock_t lock;
    struct kv_node* next[0];
}__packed;

struct sl_kv {
	int levels;
    struct kv_node *head, *tail;
};

#define sl_head(x) 	(x->head)
#define sl_tail(x) 	(x->tail)
#define sl_level(x) (x->levels - 1)

#define node_key(x)	(x->kv.k)
#define node_val(x) (x->kv.v)

extern void* sl_init();
extern void sl_destory(void* index_struct);
extern int sl_insert(void* index_struct, pkey_t key, void* val);
extern int sl_update(void* index_struct, pkey_t key, void* val);
extern int sl_remove(void* index_struct, pkey_t key);
extern void* sl_lookup(void* index_struct, pkey_t key);
extern int sl_scan(void* index_struct, pkey_t min, pkey_t max);

#endif