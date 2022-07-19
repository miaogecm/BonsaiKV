#ifndef OP_H
#define OP_H

#include "bench.h"

#ifdef ALL_IN_ONE

#define bench_lookup(kv_type, key, num_v, col_arr, size_arr, v_arr) \
        ({int i; void* __ptr; \
        for (i = 0; i < num_v; i++) { \
            key.col = col_arr[i]; \
            __ptr = bench.lookup(bench_kv, &key, sizeof(key)); \
            if (__ptr) memcpy(v_arr[i], __ptr, size_arr[i]); \
        }; \
        __ptr;})

#define bench_insert(kv_type, key, val, num_v, size_arr) \
        ({int i; void* __ptr = &val; \
        for (i = 1; i <= num_v; i++) { \
            key.col = i; \
            bench.insert(bench_kv, &key, sizeof(key), __ptr, size_arr[i]); \
            __ptr += size_arr[i]; \
        }})

#define bench_update(kv_type, key, num_v, col_arr, size_arr, v_arr) \
        ({int i; void* __ptr; \
        for (i = 0; i < num_v; i++) { \
            key.col = col_arr[i]; \
            bench.update(bench_kv, &key, sizeof(key), v_arr[i], size_arr[i]); \
        }})

#define bench_scan(kv_type, low, high, num_v, col_arr, k_arrs, v_arrs) \
        ({int i; int __arr_size; \
        for (i = 0; i < num_v; i++) { \
            low.col = col_arr[i]; high.col = col_arr[i]; \
            __arr_size = bench.scan(bench_kv, &low, &high, sizeof(low), k_arr, v_arrs[i]); \
        } \
        __arr_size;})

#if 0
#define bench_lookup(kv_type, key, val) \
        ({void* __ptr = bench.lookup(bench_kv, &key, sizeof(key)); \
        if (__ptr) memcpy(val, __ptr, sizeof(struct kv_type##_v)); \
        __ptr;})

#define bench_insert(kv_type, key, val) \
        bench.insert(bench_kv, &key, sizeof(key), &val, sizeof(val))

#define bench_update(kv_type, key, val) \
        bench.update(bench_kv, &key, sizeof(key), &val, sizeof(val))

#define bench_scan(kv_type, low, high, k_arr, v_arr) \
        bench.scan(bench_kv, &low, &high, sizeof(low), k_arr, v_arr)
#endif

#else 

#endif 
/* all in one */

#endif