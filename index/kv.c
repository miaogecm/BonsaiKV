/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include "common.h"
 
void* kv_init() {

}

void kv_destory(void* struct) {

}

int kv_insert(void* struct, key_t key, pval_t value) {

	return 0;
}

int kv_update(void* struct, key_t key, pval_t value) {

	return 0;
}

int kv_remove(void* struct, key_t key) {

	return 0;
}

void* kv_lookup(void* struct, key_t key) {

	return NULL;
}

int kv_scan(void* struct, key_t min, pkey_t max) {

	return 0;
}
