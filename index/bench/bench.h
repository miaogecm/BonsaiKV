/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *
 * Benchmark helpers
 */

#ifndef INDEX_BENCH_H
#define INDEX_BENCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"

extern int in_bonsai;

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, pkey_t key, void* value);
typedef int (*update_func_t)(void* index_struct, pkey_t key, void* value);
typedef int (*remove_func_t)(void* index_struct, pkey_t key);
typedef void* (*lookup_func_t)(void* index_struct, pkey_t key);
typedef int (*scan_func_t)(void* index_struct, pkey_t low, pkey_t high);

int bench(char* index_name, init_func_t init, destory_func_t destory,
				insert_func_t insert, remove_func_t remove,
				lookup_func_t lookup, lookup_func_t lowerbound,
                scan_func_t scan);

#ifdef __cplusplus
}
#endif

#endif //INDEX_BENCH_H
