/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include <stdlib.h>

#include "common.h"

#include "kv.h"

#define N	10

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, pkey_t key, pval_t value);
typedef int (*update_func_t)(void* index_struct, pkey_t key, pval_t value);
typedef int (*remove_func_t)(void* index_struct, pkey_t key);
typedef void* (*lookup_func_t)(void* index_struct, pkey_t key);
typedef int (*scan_func_t)(void* index_struct, pkey_t low, pkey_t high);

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destroy,
				insert_func_t insert, remove_func_t remove, 
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern int bonsai_insert(pkey_t key, pval_t value);
extern int bonsai_remove(pkey_t key);

extern int thread_epoch_init();

struct toy_kv *toy;

void* kv_init() {
	toy = malloc(sizeof(struct toy_kv));
	
	rwlock_init(&toy->lock);
	INIT_LIST_HEAD(&toy->head);

	return toy;
}

void kv_destory(void* index_struct) {
	struct kv_node* knode, *tmp;
	struct toy_kv *toy = (struct toy_kv*)index_struct;

	list_for_each_entry_safe(knode, tmp, &toy->head, list) {
		list_del(&knode->list);
		free(knode);
	}

	free(toy);
}

int kv_insert(void* index_struct, pkey_t key, pval_t value) {
	struct kv_node* knode;
	struct toy_kv *toy = (struct toy_kv*)index_struct;

	knode = malloc(sizeof(knode));
	knode->kv.k = key;
	knode->kv.v = value;

	write_lock(&toy->lock);
	list_add(&knode->list, &toy->head);
	write_unlock(&toy->lock);

	return 0;
}

int kv_update(void* index_struct, pkey_t key, pval_t value) {

	return 0;
}

int kv_remove(void* index_struct, pkey_t key) {
	struct toy_kv *toy = (struct toy_kv*)index_struct;
	struct kv_node* knode;

	read_lock(&toy->lock);
	list_for_each_entry(knode, &toy->head, list) {
		if (knode->kv.k == key)
			goto out;
	}
	read_unlock(&toy->lock);
	return -ENOENT;

out:
	read_unlock(&toy->lock);

	write_lock(&toy->lock);
	list_del(&knode->list);
	write_unlock(&toy->lock);
	free(knode);

	return 0;
}

void* kv_lookup(void* index_struct, pkey_t key) {
	struct toy_kv *toy = (struct toy_kv*)index_struct;
	struct kv_node* knode;

	read_lock(&toy->lock);
	list_for_each_entry(knode, &toy->head, list) {
		if (knode->kv.k >= key) {
			read_unlock(&toy->lock);
			return &knode->kv.v;
		}
	}
	read_unlock(&toy->lock);

	return NULL;
}

int kv_scan(void* index_struct, pkey_t min, pkey_t max) {
	//struct toy_kv *toy = (struct toy_kv*)index_struct;

	return 0;
}

int main() {
	unsigned long i;

	bonsai_init("toy kv", kv_init, kv_destory, kv_insert,
				kv_remove, kv_lookup, kv_scan);

	for (i = 0; i < N; i ++) {
		bonsai_insert((pkey_t)i, (pval_t)i);
	}

	for (i = 0; i < N; i ++) {
		bonsai_remove((pkey_t)i);
	}

	bonsai_deinit();
	
	return 0;
}
