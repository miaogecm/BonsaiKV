#ifndef SPLITHASH_DRAM_COMMON_H
#define SPLITHASH_DRAM_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <errno.h>

typedef uint64_t 	pkey_t;
typedef uint64_t 	pval_t;

static int key_cmp(pkey_t a, pkey_t b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

#define cmpxchg(addr,old,x)      	__sync_val_compare_and_swap(addr,old,x)
#define cmpxchg2(addr,old,x)		__sync_bool_compare_and_swap(addr,old,x)
#define xadd(addr,n)          		__sync_add_and_fetch(addr,n)
#define xadd2(addr,n)				__sync_fetch_and_add(addr, n)

#endif //SPLITHASH_DRAM_COMMON_H
