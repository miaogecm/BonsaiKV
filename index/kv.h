#ifndef KV_H
#define KV_H

#include "common.h"
#include "list.h"
#include "spinlock.h"
#include "arch.h"
#include "util.h"

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

extern struct sl_kv* kv_init();
extern void kv_destory(struct sl_kv* sl);
extern int kv_insert(struct sl_kv* sl, pkey_t key, void* val);
extern int kv_update(struct sl_kv* sl, pkey_t key, void* val);
extern int kv_remove(struct sl_kv* sl, pkey_t key);
extern void* kv_lookup(struct sl_kv* sl, pkey_t key);
extern int kv_scan(struct sl_kv* sl, pkey_t min, pkey_t max);

#endif