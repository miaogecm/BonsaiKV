#ifndef OP_H
#define OP_H

#include "tpcc.h"

#ifdef ALL_IN_ONE

#define tpcc_lookup(kv_type, key, val) \
        ({void* __ptr = tpcc.lookup(tpcc_kv, &key, sizeof(key)); \
        if (__ptr) memcpy(val, __ptr, sizeof(struct kv_type##_v)); \
        __ptr;})

#define tpcc_insert(kv_type, key, val) \
        tpcc.insert(tpcc_kv, &key, sizeof(key), &val, sizeof(val))

#define tpcc_update(kv_type, key, val) \
        tpcc.update(tpcc_kv, &key, sizeof(key), &val, sizeof(val))

#define tpcc_scan(kv_type, low, high, k_arr, v_arr) \
        tpcc.scan(tpcc_kv, &low, &high, sizeof(low), k_arr, v_arr)

#else 

#define tpcc_lookup(name, len, val_field, keys...)   \
        struct name __##name = {keys};              \
        __##name.v = tpcc.lookup(name##_kv, &(__##name.k), len);
        typeof()
#define tpcc_insert(name, len, )

#endif 
/* all in one */

#endif