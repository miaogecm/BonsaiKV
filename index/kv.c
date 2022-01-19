/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <sched.h>

#include "common.h"
#include "kv.h"

#ifndef N
#define N			1000001
#endif

pval_t val_arr[N];

#ifndef NUM_THREAD
#define NUM_THREAD	4
#endif

#define NUM_CPU		4

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, pkey_t key, void* value);
typedef int (*update_func_t)(void* index_struct, pkey_t key, void* value);
typedef int (*remove_func_t)(void* index_struct, pkey_t key);
typedef void* (*lookup_func_t)(void* index_struct, pkey_t key);
typedef int (*scan_func_t)(void* index_struct, pkey_t low, pkey_t high);

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destroy,
				insert_func_t insert, remove_func_t remove, 
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern int bonsai_insert(pkey_t key, pval_t value);
extern int bonsai_remove(pkey_t key);
extern int bonsai_lookup(pkey_t key, pval_t* val);
extern int bonsai_scan(pkey_t low, pkey_t high, pval_t* val_arr);

extern void bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

struct toy_kv *toy;
pthread_t tids[NUM_THREAD];

void* kv_init() {
	toy = malloc(sizeof(struct toy_kv));
	
	rwlock_init(&toy->lock);
	INIT_LIST_HEAD(&toy->head);

	return toy;
}

void kv_destory(void* index_struct) {
	struct kv_node* knode, *tmp;
	struct toy_kv *__toy = (struct toy_kv*)index_struct;

	list_for_each_entry_safe(knode, tmp, &__toy->head, list) {
		list_del(&knode->list);
		free(knode);
	}

	free(toy);
}

int kv_insert(void* index_struct, pkey_t key, void* value) {
	struct kv_node* knode;
	struct toy_kv *__toy = (struct toy_kv*)index_struct;

	knode = malloc(sizeof(struct kv_node));
	knode->kv.k = key;
	knode->kv.v = (__le64)value;

	write_lock(&__toy->lock);
	list_add(&knode->list, &__toy->head);
	write_unlock(&__toy->lock);

	kv_debug("kv insert <%lu %016lx>\n", key, value);

	return 0;
}

int kv_remove(void* index_struct, pkey_t key) {
	struct toy_kv *__toy = (struct toy_kv*)index_struct;
	struct kv_node* knode;

	read_lock(&__toy->lock);
	list_for_each_entry(knode, &__toy->head, list) {
		if (knode->kv.k == key)
			goto out;
	}
	read_unlock(&__toy->lock);
	return -ENOENT;

out:
	read_unlock(&__toy->lock);

	write_lock(&__toy->lock);
	list_del(&knode->list);
	write_unlock(&__toy->lock);
	free(knode);

	kv_debug("kv remove <%lu>\n", key);

	return 0;
}

void* kv_lookup(void* index_struct, pkey_t key) {
	struct toy_kv *__toy = (struct toy_kv*)index_struct;
	struct kv_node* knode;

	read_lock(&__toy->lock);
	list_for_each_entry(knode, &__toy->head, list) {
		if (knode->kv.k >= key) {
			read_unlock(&__toy->lock);
			//kv_debug("kv lookup <%lu %016lx>\n", knode->kv.k, knode->kv.v);
			return (void*)knode->kv.v;
		}
	}
	read_unlock(&__toy->lock);

	return NULL;
}

int kv_scan(void* index_struct, pkey_t min, pkey_t max) {
	return 0;
}

static inline int get_cpu() {
	cpu_set_t mask;
	int i;

	if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1)
		perror("sched_getaffinity fail\n");

	for (i = 0; i < NUM_CPU; i++) {
		if (CPU_ISSET(i, &mask))
			return i;
	}

	return -1;
}

static inline void bind_to_cpu(int cpu) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        perror("bind cpu failed\n");
    }
}

void* thread_fun(void* arg) {
	unsigned long i;
	long id = (long)arg;
	pval_t* v;

	bind_to_cpu(id);

	kv_debug("user thread[%ld] start on cpu[%d]\n", id, get_cpu());
	
	bonsai_user_thread_init();

	for (i = (0 + N * id); i < (N + N * id); i ++) {
		assert(bonsai_insert((pkey_t)i, (pval_t)i) == 0);
		kv_debug("insert <%lu, %lu>\n", i, i);
	}
	// for (i = 0; i < N; i ++) {
	// 	bonsai_lookup((pkey_t)i, &v);
	// 	assert(v == i);
	// }

	// for (i = 0; i < N; i ++) {
	// 	assert(bonsai_remove((pkey_t)i) == 0);
	// }
	// for (i = 0; i < N; i ++) {
	// 	//i don' know why -enoent(-102) become -2, anyway
	// 	assert(bonsai_lookup((pkey_t)i, &v) == -2);
	// }

	for (int i = 0; i < 1; i++) {
		int size;

		printf("scan [%lu %lu]:\n", (pval_t)50, (pkey_t)100);
		size = bonsai_scan((pkey_t)50, (pkey_t)100, val_arr);
		for (int j = 0; j < size; j++) {
			printf("%lu ", val_arr[j]);
		}
		printf("\n");
	}
#endif
	bonsai_user_thread_exit();

	return NULL;
}

int main() {
	long i;

	if (bonsai_init("toy kv", kv_init, kv_destory, kv_insert,
				kv_remove, kv_lookup, kv_scan) < 0)
		goto out;

	for (i = 3; i < NUM_THREAD; i++) {
		pthread_create(&tids[i], NULL, thread_fun, (void*)i);
	}

	sleep(20);

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_join(tids[i], NULL);
	}

	thread_fun(1);
	
	// bonsai_deinit();

out:
	return 0;
}
