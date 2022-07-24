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

#include "spinlock.h"

#include "bench.h"
#include "valman.h"
#include "common.h"

#include "compressor.h"
#include "decompressor.h"

#include "pcm.h"

//#define REMOTE_PMEM_ACCESS_STATISTIC

#define LOAD_PATH   "../../index-microbench/workloads/load"
#define OP_PATH     "../../index-microbench/workloads/op"

static ycsb_decompressor_t load_dec, op_dec;

#ifndef NUM_THREAD
#define NUM_THREAD	48
#endif

#define NUM_CPU		48

#define VCLASS      VCLASS_16B
#define VAL_LEN     16

static void           *index_struct;
static init_func_t    fn_init;
static destory_func_t fn_destroy;
static insert_func_t  fn_insert;
static update_func_t  fn_update;
static remove_func_t  fn_remove;
static lookup_func_t  fn_lookup;
static lookup_func_t  fn_lowerbound;
static scan_func_t    fn_scan;

typedef enum {
    SCAN_NEXT,
    SCAN_STOP
} scanner_ctl_t;

typedef scanner_ctl_t (*scanner_t)(pentry_t e, void* argv);

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
extern int bonsai_insert_commit(pkey_t key, pval_t value);
extern int bonsai_remove_commit(pkey_t key);
extern int bonsai_lookup(pkey_t key, pval_t *val);
extern int bonsai_scan(pkey_t start, int range, pval_t *values);

extern void bonsai_dtx_start();
extern void bonsai_dtx_commit();

extern int bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

#define MK_K(key, len)      bonsai_make_key(key, len)

pthread_t tids[NUM_THREAD];

int in_bonsai;

static inline uint64_t atoul(const char* str) {
	uint64_t res = 0;
	unsigned int i;

	for (i = 0; i < strlen(str); i++) {
		res = res * 10 + str[i] - '0';
	}

	return res;
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

#ifdef STR_VAL
static inline char *get_val(pkey_t key) {
    return "";
}
#else
static inline uint64_t get_val(pkey_t key) {
    return 0;
}
#endif

static void do_op(const char *name, ycsb_decompressor_t *dec, long id) {
	pval_t v = 0;
	pval_t* val_arr;
    double interval;
	long i, repeat = 1;
    int st, ed;
    enum op_type op;
    pkey_t pkey;
    pval_t pval;
    size_t size;
    int ret, nr, range;
#ifdef STR_KEY
    uint64_t key;
#else
    char *key;
#endif

    nr = ycsb_decompressor_get_nr(dec);

    val_arr = malloc(sizeof(pval_t*) * nr);

    st = 1.0 * id / NUM_THREAD * nr;
    ed = 1.0 * (id + 1) / NUM_THREAD * nr;

    start_measure();

    while(repeat--) {
        for (i = st; i < ed; i ++) {
            op = ycsb_decompressor_get(dec, &key, &range, i);
#ifdef STR_KEY
            pkey = MK_K(key, strlen(key));
#else
            pkey = INT2KEY(key);
#endif

            switch (op) {
            case OP_INSERT:
            case OP_UPDATE:
                if (in_bonsai) {
#ifdef STR_VAL
                    pval = bonsai_make_val(VCLASS, get_val(pkey));
#else
                    pval = get_val(pkey);
#endif
                    ret = bonsai_insert_commit(pkey, pval);
                } else {
                    // ret = fn_insert(index_struct, op_arr[i][1], 8, (void *) op_arr[i][2]);
                }
                assert(ret == 0);
                break;
            case 2:
                if (in_bonsai) {
                    ret = bonsai_lookup(pkey, &v);
#ifdef STR_VAL
                    pval = (pval_t)bonsai_extract_val(&size, v);
                    bonsai_free_val(v);
#else

#endif
                } else {
                    // v = (pval_t) fn_lookup(index_struct, op_arr[i][1], 8, NULL);
                }
                asm volatile("" : : "r"(v) : "memory");
                break;
            case 3:
                if (in_bonsai) {
                    bonsai_scan(pkey, range, val_arr);
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
    printf("user thread[%ld]: %s finished in %.3lf seconds\n", id, name, interval);

	printf("user thread[%ld]---------------------end---------------------\n", id);

	free(val_arr);
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

#ifdef REMOTE_PMEM_ACCESS_STATISTIC
    if (id == 0) {
        pcm_on();
    }
#endif

    if (in_bonsai) {
        bonsai_user_thread_init(tids[id]);
    }

    pthread_barrier_wait(&barrier);

    bonsai_online();
    do_op("load", &load_dec, id);
    bonsai_offline();

    pthread_barrier_wait(&barrier);

    do_barrier(id, "load");

#ifdef REMOTE_PMEM_ACCESS_STATISTIC
    if (id == 0) {
        pcm_start();
    }
#endif

    pthread_barrier_wait(&barrier);

    bonsai_online();

    do_op("op", &op_dec, id);

#ifdef REMOTE_PMEM_ACCESS_STATISTIC
    if (id == 0) {
        uint64_t nr = pcm_get_nr_remote_pmem_access_packet();
        printf("remote pmem traffic: %lu packets\n", nr);
    }
#endif

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
    int cpu, is_str_key;
    char *use_bonsai;

#ifdef STR_KEY
    is_str_key = 1;
#else
    is_str_key = 0;
#endif

    ycsb_decompressor_init(&load_dec, LOAD_PATH, is_str_key);
    ycsb_decompressor_init(&op_dec, OP_PATH, is_str_key);

    use_bonsai = getenv("bonsai");
    in_bonsai = use_bonsai && !strcmp(use_bonsai, "yes");

    in_bonsai = 1;
    if (in_bonsai) {
        printf("Using bonsai.\n");
    }
#ifdef STR_KEY
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

    ycsb_decompressor_deinit(&load_dec);
    ycsb_decompressor_deinit(&op_dec);

out:
	return 0;
}
