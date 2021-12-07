#ifndef KV_H
#define KV_H

#include "common.h"
#include "list.h"
#include "rwlock.h"

struct kv_node {
  pentry_t kv;
  struct list_head list;
}

struct toy_kv {
	rwlock_t lock;
  struct list_head head;
};

extern void* kv_init();
extern void kv_destory(void* struct);
extern int kv_insert(void* struct, pkey_t key, pval_t value);
extern int kv_update(void* struct, pkey_t key, pval_t value);
extern int kv_remove(void* struct, pkey_t key);
extern void* kv_lookup(void* struct, pkey_t key);
extern int kv_scan(void* struct, pkey_t min, pkey_t max);

#endif
