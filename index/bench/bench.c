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

#include "bench.h"

#include "../data/kvdata.h"

#ifndef N
#define N			1000000
#endif

pkey_t a[N];

#ifndef NUM_THREAD
#define NUM_THREAD	4
#endif

#define NUM_CPU		8

static void           *index_struct;
static init_func_t    fn_init;
static destory_func_t fn_destroy;
static insert_func_t  fn_insert;
static remove_func_t  fn_remove;
static lookup_func_t  fn_lookup;
static lookup_func_t  fn_lowerbound;
static scan_func_t    fn_scan;

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

int in_bonsai;

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
    int ret;

    if (id == 0) {
        start_measure();
        for (i = 0; i < N; i ++) {
            if (in_bonsai) {
                ret = bonsai_insert(load_arr[i][0], load_arr[i][1]);
            } else {
                ret = fn_insert(index_struct, load_arr[i][0], (void *) load_arr[i][1]);
            }
            assert(ret == 0);
        }
        interval = end_measure();
        printf("load finished in %.3lf seconds\n", interval);

        if (in_bonsai) {
            bonsai_barrier();
        }

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
                if (in_bonsai) {
                    bonsai_lookup(op_arr[id][i][1], &v);
                } else {
                    v = (pval_t) fn_lookup(index_struct, op_arr[id][i][1]);
                }
                break;
            case 3:
                if (in_bonsai) {
                    bonsai_scan(op_arr[id][i][1], op_arr[id][i][2], val_arr);
                } else {
                    // TODO: Implement it
                    assert(0);
                }
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

    if (in_bonsai) {
        bonsai_user_thread_exit();
    }

	free(val_arr);

	printf("user thread[%ld] exit\n", id);
}

static void do_barrier(long id) {
    double interval;

    if (id == 0) {
        if (in_bonsai) {
            bonsai_barrier();
        }

        interval = end_measure();
        printf("op total: %.3lf seconds\n", interval);
    }
}

void* thread_fun(void* arg) {
	long id = (long)arg;

	bind_to_cpu(id);
    bonsai_debug("user thread[%ld] start on cpu[%d]\n", id, get_cpu());

    if (in_bonsai) {
        bonsai_user_thread_init();
    }

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
				lookup_func_t lookup, lookup_func_t lowerbound,
                scan_func_t scan) {
    pthread_t user_thread_parent;
    char *use_bonsai;

    use_bonsai = getenv("bonsai");
    in_bonsai = use_bonsai && !strcmp(use_bonsai, "yes");

    if (in_bonsai) {
        printf("Using bonsai.\n");
    }

    fn_init = init;
    fn_destroy = destory;
    fn_insert = insert;
    fn_remove = remove;
    fn_lookup = lookup;
    fn_lowerbound = lowerbound;
    fn_scan = scan;

	bind_to_cpu(0);

    pthread_barrier_init(&barrier, NULL, NUM_THREAD);

    if (in_bonsai) {
        if (bonsai_init(index_name, init, destory, insert, remove, lowerbound, scan) < 0)
            goto out;
    } else {
        index_struct = init();
    }

    pthread_create(&user_thread_parent, NULL, user_thread_parent_fun, NULL);
    pthread_setname_np(user_thread_parent, "user_thread_parent");
    pthread_join(user_thread_parent, NULL);

    if (in_bonsai) {
        bonsai_deinit();
    } else {
        destory(index_struct);
    }

out:
	return 0;
}
