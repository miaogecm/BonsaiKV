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
#include <time.h>
#include <errno.h>

#include "common.h"
#include "kv.h"

// #define RAND

#ifndef N
#define N			100001
#endif

pkey_t a[5 * N];

#ifndef NUM_THREAD
#define NUM_THREAD	4
#endif

#define NUM_CPU		8

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

struct kv_node* __kv_lookup(void* index_struct, pkey_t key) {
	struct toy_kv *__toy = (struct toy_kv*)index_struct;
	struct kv_node *knode;

	list_for_each_entry(knode, &__toy->head, list) {
		if (knode->kv.k >= key) {
			break;
		}
	}

	return knode;
}

/*must support update*/
int kv_insert(void* index_struct, pkey_t key, void* value) {
	struct kv_node *knode, *next_node;
	struct toy_kv *__toy = (struct toy_kv*)index_struct;
	struct list_head* ll_node;

	knode = malloc(sizeof(struct kv_node));
	knode->kv.k = key;
	knode->kv.v = (__le64)value;

	write_lock(&__toy->lock);
	next_node = __kv_lookup(index_struct, key);
	if (&next_node->list != &__toy->head && next_node->kv.k == key) {
		next_node->kv.v = value;
	} else {
		ll_node = &next_node->list;
		__list_add(&knode->list, ll_node->prev, ll_node);
	}
	write_unlock(&__toy->lock);

	//bonsai_debug("kv insert <%lu %016lx>\n", key, value);

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

	bonsai_debug("kv remove <%lu>\n", key);

	return 0;
}

void* kv_lookup(void* index_struct, pkey_t key) {
	struct toy_kv *__toy = (struct toy_kv*)index_struct;
	struct kv_node* knode;

	read_lock(&__toy->lock);
	list_for_each_entry(knode, &__toy->head, list) {
		if (knode->kv.k >= key) {
			read_unlock(&__toy->lock);
			//bonsai_debug("kv lookup <%lu %016lx>\n", knode->kv.k, knode->kv.v);
			return (void*)knode->kv.v;
		}
	}
	read_unlock(&__toy->lock);

	return NULL;
}

int kv_scan(void* index_struct, pkey_t min, pkey_t max) {
	return 0;
}

void kv_print(void* index_struct) {
	struct toy_kv *__toy = (struct toy_kv*)index_struct;
	struct kv_node* knode;

	list_for_each_entry(knode, &__toy->head, list) {
		printf("<%lu, %016lx> -> ", knode->kv.k, knode->kv.v);
	}
	printf("NULL\n");
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

extern void bonsai_print_all();
void* thread_fun(void* arg) {
	unsigned long i;
	long id = (long)arg;
	pval_t v = 0;
	pval_t* val_arr = malloc(sizeof(pval_t*) * 2 * N);

	bind_to_cpu(id);

	bonsai_debug("user thread[%ld] start on cpu[%d]\n", id, get_cpu());
	
	bonsai_user_thread_init();

	for (i = (0 + N * id); i < (N + N * id); i ++) {
		assert(bonsai_insert((pkey_t)i, (pval_t)i) == 0);
	}

	// bonsai_print_all();
	//printf("thread[%ld]---------------------1---------------------\n", id);

	// for (i = (0 + N * id); i < (N + N * id); i ++) {
	// 	bonsai_lookup((pkey_t)a[i], &v);
	// 	assert(v == a[i]);
	// }

	//printf("thread[%ld]---------------------2---------------------\n", id);

	// for (int i = 0; i < 1; i++) {
	// 	int size;
	// 	//printf("scan [%lu %lu]:\n", (pval_t)(0 + N * id), (pkey_t)(N + N * id - 1));
	// 	size = bonsai_scan((pkey_t)(0 + N * id), (pkey_t)(N + N * id - 1), val_arr);
	// 	assert(size == N);
	// }

	//printf("thread[%ld]---------------------3---------------------\n", id);

	for (i = (0 + N * id); i < (N + N * id); i ++) {
		assert(bonsai_remove((pkey_t)i) == 0);
	}

	//printf("thread[%ld]---------------------4---------------------\n", id);

	// for (i = (0 + N * id); i < (N + N * id); i ++) {
	// 	assert(bonsai_lookup((pkey_t)i, &v) == -ENOENT);
	// }

	//printf("thread[%ld]---------------------5---------------------\n", id);

	bonsai_user_thread_exit();

	return NULL;
}

void gen_data() {
	unsigned long i;

	for (i = 0; i < NUM_THREAD * N; i++) {
		a[i] = (pkey_t)i;
	}
#ifdef RAND
	srand(time(0));
	for (i = 1; i < NUM_THREAD * N; i++) {
        int swap_pos = rand() % (NUM_THREAD * N - i) + i;
        pkey_t temp = a[i];
        a[i] = a[swap_pos];
        a[swap_pos] = temp;
    }
#endif
}

int main() {
	long i;

	bind_to_cpu(0);

	if (bonsai_init("toy kv", kv_init, kv_destory, kv_insert,
				kv_remove, kv_lookup, kv_scan) < 0)
		goto out;

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_create(&tids[i], NULL, thread_fun, (void*)i);
	}

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_join(tids[i], NULL);
	}
	
	bonsai_deinit();

out:
	return 0;
}