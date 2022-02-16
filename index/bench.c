/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *
 * Benchmark helpers
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
#include <string.h>
#include <sys/time.h>

#include "ffwrapper.h"
#include "bench.h"

#include "data/kvdata.h"

#ifndef N
#define N			1000000
#endif

pkey_t a[N];

#ifndef NUM_THREAD
#define NUM_THREAD	4
#endif

#define NUM_CPU		8

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destory,
				insert_func_t insert, remove_func_t remove,
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_barrier();

extern int bonsai_insert(pkey_t key, pval_t value);
extern int bonsai_remove(pkey_t key);
extern int bonsai_lookup(pkey_t key, pval_t* val);
extern int bonsai_scan(pkey_t low, pkey_t high, pval_t* val_arr);

extern void bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

struct toy_kv *toy;
pthread_t tids[NUM_THREAD];

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

static pthread_barrier_t barrier;

static __thread struct timeval t0, t1;

static inline void start_measure() {
    gettimeofday(&t0, NULL);
}

static inline double end_measure() {
    gettimeofday(&t1, NULL);
    return t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
}

static void do_load(long id) {
    double interval;
	long i;
    if (id == 0) {
        start_measure();
        for (i = 0; i < N; i ++) {
            assert(bonsai_insert(load_arr[i][0], load_arr[i][1]) == 0);
        }
        interval = end_measure();
        printf("load finished in %.3lf seconds\n", interval);

        bonsai_barrier();

        interval = end_measure();
        printf("load total: %.3lf seconds\n", interval);
    }
}

static void do_op(long id) {
	pval_t v = 0;
	pval_t* val_arr = malloc(sizeof(pval_t*) * N);
    double interval;
	long i, repeat = 10;

    start_measure();

    while(repeat--) {
        for (i = 0; i < N; i ++) {

            switch (op_arr[id][i][0]) {
            case 0:
                // bonsai_insert(op_arr[id][i][1], op_arr[id][i][2]);
                // break;
            case 1:
                // bonsai_insert(op_arr[id][i][1], op_arr[id][i][2]);
                // break;
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
    }

    interval = end_measure();
    printf("user thread[%ld]: workload finished in %.3lf seconds\n", id, interval);

	printf("user thread[%ld]---------------------end---------------------\n", id);

	bonsai_user_thread_exit();

	free(val_arr);

	printf("user thread[%ld] exit\n", id);
}

static void do_barrier(long id) {
    double interval;

    if (id == 0) {
        bonsai_barrier();

        interval = end_measure();
        printf("op total: %.3lf seconds\n", interval);
    }
}

void* thread_fun(void* arg) {
	long id = (long)arg;

	bind_to_cpu(id);
    bonsai_debug("user thread[%ld] start on cpu[%d]\n", id, get_cpu());

	bonsai_user_thread_init();

    pthread_barrier_wait(&barrier);

    do_load(id);

    pthread_barrier_wait(&barrier);

    do_op(id);

    pthread_barrier_wait(&barrier);

    do_barrier(id);

	return NULL;
}

void *user_thread_parent_fun(void *arg) {
    long i;
	bind_to_cpu(0);
    for (i = 0; i < NUM_THREAD; i++) {
		pthread_create(&tids[i], NULL, thread_fun, (void*)i);
        pthread_setname_np(tids[i], "user_thread");
	}
	for (i = 0; i < NUM_THREAD; i++) {
		pthread_join(tids[i], NULL);
	}
}

int bench(char* index_name, init_func_t init, destory_func_t destory,
				insert_func_t insert, remove_func_t remove,
				lookup_func_t lookup, scan_func_t scan) {
    pthread_t user_thread_parent;

	bind_to_cpu(0);

    pthread_barrier_init(&barrier, NULL, NUM_THREAD);

	if (bonsai_init(index_name, init, destory, insert, remove, lookup, scan) < 0)
		goto out;

    pthread_create(&user_thread_parent, NULL, user_thread_parent_fun, NULL);
    pthread_setname_np(user_thread_parent, "user_thread_parent");
    pthread_join(user_thread_parent, NULL);

	bonsai_deinit();

out:
	return 0;
}
