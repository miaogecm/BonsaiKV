/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include <stdlib.h>

#include "common.h"
 
void* kv_init() {
	struct toy_kv *toy;

	toy = malloc(sizeof(struct toy_kv));
	
	rwlock_init(&toy->lock);
	INIT_LIST_HEAD(&toy->head);
}

void kv_destory(struct toy_kv *toy) {
	struct kv_node* knode, *tmp;

	list_for_each_entry_safe(knode, tmp, &toy->head, list) {
		list_del(&knode->list);
		free(knode);
	}

	free(struct);
}

int kv_insert(struct toy_kv *toy, key_t key, pval_t value) {
	struct knode* knode;

	knode = malloc(sizeof(knode));
	knode->kv.k = key;
	knode->kv.v = value;

	write_lock(&toy->lock);
	list_add(&knode->list, &toy->head);
	write_unlock(&toy->lock);

	return 0;
}

int kv_update(struct toy_kv *toy, key_t key, pval_t value) {

	return 0;
}

int kv_remove(struct toy_kv *toy, key_t key) {
	struct knode* knode;

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
	list_del(knode);
	write_unlock(&toy->lock);
	free(knode);

	return 0;
}

void* kv_lookup(struct toy_kv *toy, key_t key) {
	struct knode* knode;

	read_lock(&toy->lock);
	list_for_each_entry(knode, &toy->head, list) {
		if (knode->kv.k == key)
			break;
	}
	read_unlock(&toy->lock);

	return &knode->kv.v;
}

int kv_scan(void* struct, key_t min, pkey_t max) {

	return 0;
}
