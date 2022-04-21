#ifndef TPCC_H
#define TPCC_H

#include "spinlock.h"
#include "atomic.h"

#define MAX_THREADS 128
#define ALL_IN_ONE

typedef void* kv;

typedef void* (*init_func_t)(void);
typedef void (*destroy_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, const void *key, size_t len, const void* value, size_t v_len);
typedef int (*update_func_t)(void* index_struct, const void *key, size_t len, const void* value, size_t v_len);
typedef int (*remove_func_t)(void* index_struct, const void *key, size_t len);
typedef void* (*lookup_func_t)(void* index_struct, const void *key, size_t len);
typedef int (*scan_func_t)(void* index_struct, const void *low, const void *high, size_t len, size_t* k_arr, size_t* v_arr);

struct tpcc {
#ifdef ALL_IN_ONE
    kv  kv;
#else
    kv  kv[9];
#endif
    spinlock_t  table_lock[9];

    init_func_t     init;
    destroy_func_t  destroy;
    insert_func_t   insert;
    update_func_t   update;
    remove_func_t   remove;
    lookup_func_t   lookup;
    scan_func_t     scan;

    atomic_t h_pk[MAX_THREADS];
};

extern struct tpcc tpcc;
extern int num_w;

#ifdef ALL_IN_ONE
#define tpcc_kv         (tpcc.kv)
#else 
#define warehouse_kv    (tpcc.kv[0])
#define district_kv     (tpcc.kv[1])
#define customer_kv     (tpcc.kv[2])
#define history_kv      (tpcc.kv[3])
#define order_kv        (tpcc.kv[4])
#define neworder_kv     (tpcc.kv[5])
#define orderline_kv    (tpcc.kv[6])
#define stock_kv        (tpcc.kv[7])
#define item_kv         (tpcc.kv[8])
#endif

#define warehouse_lock  (&tpcc.table_lock[0])
#define district_lock   (&tpcc.table_lock[1])
#define customer_lock   (&tpcc.table_lock[2])
#define history_lock    (&tpcc.table_lock[3])
#define order_lock      (&tpcc.table_lock[4])
#define neworder_lock   (&tpcc.table_lock[5])
#define orderline_lock  (&tpcc.table_lock[6])
#define stock_lock      (&tpcc.table_lock[7])
#define item_lock       (&tpcc.table_lock[8])

extern void tpcc_init(init_func_t init, destroy_func_t destroy,
				      insert_func_t insert, update_func_t update, remove_func_t remove,
				      lookup_func_t lookup, scan_func_t scan);
extern void tpcc_destroy();
extern void tpcc_set_env(int __num_w, int __num_works, int __num_cpu);
extern void tpcc_load(int __num_threads);
extern void tpcc_bench(int __num_threads);

#endif