/*
 * Bonsai: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
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

#define USE_MAP
#include "map.c"
#include "spinlock.h"

#include "bench.h"

#include "../data/kvdata.h"

#ifndef N
#define N			1000000
#endif

pkey_t a[N];

#ifndef NUM_THREAD
#define NUM_THREAD	2
#endif

#define NUM_CPU		12

static void           *index_struct;
static init_func_t    fn_init;
static destory_func_t fn_destroy;
static insert_func_t  fn_insert;
static update_func_t  fn_update;
static remove_func_t  fn_remove;
static lookup_func_t  fn_lookup;
static lookup_func_t  fn_lowerbound;
static scan_func_t    fn_scan;

extern int
bonsai_init(char *index_name, init_func_t init, destory_func_t destory, insert_func_t insert, update_func_t update,
            remove_func_t remove, lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_mark_cpu(int cpu);
extern void bonsai_barrier();

extern void bonsai_online();
extern void bonsai_offline();

extern pkey_t bonsai_make_key(const void *key, size_t len);

extern int bonsai_insert(pkey_t key, pval_t value);
extern int bonsai_remove(pkey_t key);
extern int bonsai_lookup(pkey_t key, pval_t *val);
extern int bonsai_scan(pkey_t low, uint16_t lo_len, pkey_t high, uint16_t hi_len, pval_t* val_arr);

extern int bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

pthread_t tids[NUM_THREAD];

int in_bonsai;

hashmap* map;

static void map_load()  {
    int i;
    pkey_t* __key;

    for (i = 0; i < N; i ++) {
        __key = malloc(sizeof(pkey_t));
#ifdef LONG_KEY
        *__key = MK_K(load_arr[i][0], strlen(load_arr[i][0]));
#else
        *__key = INT2KEY(load_arr[i][0]);
#endif
        hashmap_set(map, __key, KEY_LEN, load_arr[i][1]);
    }
}

static void map_lookup_check(pkey_t k, pval_t exc_v) {
#ifdef USE_MAP
    pval_t v = 0;
    assert(hashmap_get(map, &k, KEY_LEN, &v));
    assert(exc_v == v);
#endif
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
	long i, st, ed;
    int ret;
    int repeat = 1;
    pkey_t __key;

	//printf("user thread[%ld] do load\n", id);

    start_measure();

    st = 1.0 * id / NUM_THREAD * N;
    ed = 1.0 * (id + 1) / NUM_THREAD * N;

    while(repeat--) {
        for (i = st; i < ed; i ++) {
            if (in_bonsai) {
#ifdef LONG_KEY
                __key = MK_K(load_arr[i][0], strlen(load_arr[i][0]));
#else
                __key = INT2KEY(load_arr[i][0]);
#endif
                ret = bonsai_insert(__key, load_arr[i][1]);
            } else {
                ret = fn_insert(index_struct, load_arr[i][0], 8, (void *) load_arr[i][1]);
            }
            assert(ret == 0);
        }
    }
    interval = end_measure();
    printf("user thread[%ld] load finished in %.3lf seconds\n", id, interval);
}

static void do_op(long id) {
#if 1
	pval_t v = 0;
	pval_t* val_arr = malloc(sizeof(pval_t*) * N);
    double interval;
	long i, repeat = 1;
    int st, ed, opcode;
    pkey_t __key, __key2;
    int ret;

    st = 1.0 * id / NUM_THREAD * N;
    ed = 1.0 * (id + 1) / NUM_THREAD * N;

    start_measure();

    while(repeat--) {
        for (i = st; i < ed; i ++) {
#ifdef LONG_KEY
            opcode = op_arr[i][0][0] - '0';
#else
            opcode = op_arr[i][0];
#endif
            switch (opcode) {
            case 0:
            case 1:
                if (in_bonsai) {
#ifdef LONG_KEY
                    __key = MK_K(op_arr[i][1], strlen(op_arr[i][1]));
#else
                    __key = INT2KEY(op_arr[i][1]);
#endif
                    ret = bonsai_insert(__key, op_arr[i][2]);
					
                } else {
                    ret = fn_insert(index_struct, op_arr[i][1], 8, (void *) op_arr[i][2]);
                }
                assert(ret == 0);
                break;
            case 2:
                if (in_bonsai) {
#ifdef LONG_KEY
                    __key = MK_K(op_arr[i][1], strlen(op_arr[i][1]));
#else
                    __key = INT2KEY(op_arr[i][1]);
#endif
                    v = 0;
                    ret = bonsai_lookup(__key, &v);				
                    map_lookup_check(__key, v);
                } else {
                    v = (pval_t) fn_lookup(index_struct, op_arr[i][1], 8, NULL);
                }
                asm volatile("" : : "r"(v) : "memory");
                break;
            case 3:
                if (in_bonsai) {
#ifdef LONG_KEY
                    __key = MK_K(op_arr[i][1], strlen(op_arr[i][1]));
                    __key2 = MK_K(op_arr[i][2], strlen(op_arr[i][2]));
#else
                    __key = INT2KEY(op_arr[i][1]);
                    __key2 = INT2KEY(op_arr[i][2]);
#endif
                    bonsai_scan(__key, 8, __key2, 8, val_arr);
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

	free(val_arr);
#endif
}

static void do_barrier(long id, const char* name) {
    double interval;

    if (id == 0) {
        if (in_bonsai) {
            bonsai_barrier();
        }

        interval = end_measure();
        printf("%s total: %.3lf seconds\n", name, interval);
    }
}

void* thread_fun(void* arg) {
	long id = (long)arg;

	bind_to_cpu(id);
    bonsai_debug("user thread[%ld] start on cpu[%d]\n", id, get_cpu());

    if (in_bonsai) {
        bonsai_user_thread_init(tids[id]);
    }

    pthread_barrier_wait(&barrier);

    bonsai_online();
    do_load(id);
    bonsai_offline();

    pthread_barrier_wait(&barrier);

    do_barrier(id, "load");

    pthread_barrier_wait(&barrier);

    bonsai_online();
    do_op(id);
    bonsai_offline();

    pthread_barrier_wait(&barrier);

    do_barrier(id, "op");

    pthread_barrier_wait(&barrier);

    if (in_bonsai) {
        bonsai_user_thread_exit();
    }

	printf("user thread[%ld] exit\n", id);

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
	return NULL;
}

int bench(char* index_name, init_func_t init, destory_func_t destory,
          insert_func_t insert, update_func_t update, remove_func_t remove,
          lookup_func_t lookup, lookup_func_t lowerbound, scan_func_t scan) {
    pthread_t user_thread_parent;
    char *use_bonsai;
    int cpu;

    use_bonsai = getenv("bonsai");
    in_bonsai = use_bonsai && !strcmp(use_bonsai, "yes");

    in_bonsai = 1;
    if (in_bonsai) {
        printf("Using bonsai.\n");
    }
#ifdef LONG_KEY
    printf("Using long key.\n");
#endif

    fn_init = init;
    fn_destroy = destory;
    fn_insert = insert;
    fn_update = update;
    fn_remove = remove;
    fn_lookup = lookup;
    fn_lowerbound = lowerbound;
    fn_scan = scan;

	bind_to_cpu(0);

    pthread_barrier_init(&barrier, NULL, NUM_THREAD);

    if (in_bonsai) {
        for (cpu = 0; cpu < NUM_THREAD; cpu++) {
            bonsai_mark_cpu(cpu);
        }

        if (bonsai_init(index_name, init, destory, insert, update, remove, lowerbound, scan) < 0)
            goto out;
    } else {
        index_struct = init();
    }

#ifdef USE_MAP
    map = hashmap_create();
    map_load();
#endif

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
