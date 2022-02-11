/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 
 * A toy key-value store
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
#include <sys/time.h>

#include "common.h"
#include "ffwrapper.h"

#include "data/kvdata.h"
// #define RAND

#ifndef N
#define N			1000000
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

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destory,
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

void kv_print(void* p) {

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

static inline void die() {
	assert(0);
	exit(1);
}

void* thread_fun(void* arg) {
	long i, id = (long)arg;
	pval_t v = 0;
	pval_t* val_arr = malloc(sizeof(pval_t*) * N);

	bind_to_cpu(id);

	bonsai_debug("user thread[%ld] start on cpu[%d]\n", id, get_cpu());
	
	bonsai_user_thread_init();

	for (i = 0; i < N; i ++) {

        switch (op_arr[id][i][0]) {
        case 0:
            bonsai_insert(op_arr[id][i][1], op_arr[id][i][2]);
            break;
        case 1:
            bonsai_insert(op_arr[id][i][1], op_arr[id][i][2]);
            break;        
        case 2:
            bonsai_lookup(op_arr[id][i][1], &v);
            break;
        case 3:
            bonsai_scan(op_arr[id][i][1], op_arr[id][i][2], val_arr);
            break;
        default:
            printf("unknown type\n");
            assert(0);
            break;
        }
	}

	// printf("user thread[%ld]---------------------end---------------------\n", id);

	bonsai_user_thread_exit();

	free(val_arr);
    
	// printf("user thread[%ld] exit\n", id);

	return NULL;
}

int main() {
	long i;
    struct timeval t0, t1;

	bind_to_cpu(0);

	if (bonsai_init("skiplist", ff_init, ff_destory, ff_insert,
				ff_remove, ff_lookup, ff_scan) < 0)
		goto out;

    struct timeval t;
    // gettimeofday();
    printf("start load [%d] entries\n", N);
    for (i = 0; i < N; i ++) {
        assert(bonsai_insert(load_arr[i][0], load_arr[i][1]) == 0);
    }
    printf("load succeed!\n");
    sleep(10);
    printf("workload start!\n");

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_create(&tids[i], NULL, thread_fun, (void*)i);
	}

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_join(tids[i], NULL);
	}
    
    sleep(10);
	bonsai_deinit();

out:
    printf("end\n");
	return 0;
}