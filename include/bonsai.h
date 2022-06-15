#ifndef _BONSAI_H
#define _BONSAI_H

#ifdef __cplusplus
extern "C" {
#endif

#include "cpu.h"

#include <libpmemobj.h>
#include <thread.h>
#include <assert.h>
#include <immintrin.h>

#include "atomic.h"
#include "region.h"
#include "list.h"
#include "common.h"
#include "spinlock.h"
#include "log_layer.h"
#include "arch.h"
#include "bitmap.h"
#include "rwlock.h"
#include "index_layer.h"
#include "rcu.h"
#include "valman.h"

#define REGION_FPATH_LEN	19
#define OBJPOOL_FPATH_LEN	20

struct bonsai_desc {
	__le32 init;
	__le32 padding;
	__le64 epoch;
	struct log_region_desc log_region[NUM_CPU];
	char log_region_fpath[NUM_SOCKET][REGION_FPATH_LEN];
}__packed;

struct bonsai_info {
	/* 1. bonsai info */
	int	fd;
	struct bonsai_desc	*desc;

	/* 2. layer info */
    struct index_layer 	i_layer;
    struct shim_layer   s_layer;
    struct log_layer 	l_layer;
    struct data_layer 	d_layer;

	/* 3. thread info */
	struct thread_info* self;
	struct list_head	thread_list;
	spinlock_t          list_lock;

	pthread_t 			tids[1 + NUM_PFLUSH_THREAD + NUM_SMO_THREAD + NUM_USER_THREAD];

	/* pflushd */
	struct thread_info *pflush_threads[0];
	struct thread_info *pflush_master;
	struct thread_info *pflush_workers[NUM_SOCKET][NUM_PFLUSH_WORKER_PER_NODE];
	struct thread_info *user_threads[NUM_USER_THREAD];

    /* smo */
	struct thread_info *smo;

    /* RCU */
    rcu_t rcu;

	/* DTX */
	atomic_t tx_id;
}____cacheline_aligned;

#define INDEX(bonsai)    	(&(bonsai->i_layer))
#define SHIM(bonsai)        (&(bonsai->s_layer))
#define LOG(bonsai)   		(&(bonsai->l_layer))
#define DATA(bonsai)  		(&(bonsai->d_layer))
#define RCU(bonsai)         (&(bonsai->rcu))

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, struct pnode);
POBJ_LAYOUT_END(BONSAI);

extern struct bonsai_info* bonsai;

static inline int pkey_compare(pkey_t a, pkey_t b) {
#ifdef STR_KEY
#if KEY_LEN != 24
#error "Unsupported key length!"
#endif
    unsigned long pos;
    __m256i ma, mb;
    ma = _mm256_loadu_si256((const __m256i *) a.key);
    mb = _mm256_loadu_si256((const __m256i *) b.key);
    pos = ffz(_mm256_movemask_epi8(_mm256_cmpeq_epi8(ma, mb)));
    /* signed compare */
    return a.key[pos] - b.key[pos];
#else
#if KEY_LEN != 8
#error "Unsupported key length!"
#endif
    unsigned long x = *(unsigned long *) a.key;
    unsigned long y = *(unsigned long *) b.key;
    if (x < y) {
        return -1;
    }
    if (x > y) {
        return 1;
    }
    return 0;
#endif
}

static inline pkey_t pkey_to_str(pkey_t k) {
#ifndef STR_KEY
#if KEY_LEN != 8
#error "Unsupported key length!"
#endif
    unsigned long *p = (unsigned long *) k.key;
    *p = __builtin_bswap64(*p);
#endif
    return k;
}

static inline pkey_t str_to_pkey(pkey_t k) {
#ifndef STR_KEY
#if KEY_LEN != 8
#error "Unsupported key length!"
#endif
    unsigned long *p = (unsigned long *) k.key;
    *p = __builtin_bswap64(*p);
#endif
    return k;
}

static inline pkey_t pkey_prev(pkey_t k) {
#ifdef STR_KEY
#if KEY_LEN != 24
#error "Unsupported key length!"
#endif
    k.key[KEY_LEN - 1]--;
    return k;
#else
#if KEY_LEN != 8
#error "Unsupported key length!"
#endif
    unsigned long val = *(unsigned long *) k.key - 1;
    return *(pkey_t *) &val;
#endif
}

static inline uint8_t pkey_get_signature(pkey_t k) {
    uint8_t res;
#ifdef STR_KEY
#if KEY_LEN != 24
#error "Unsupported key length!"
#endif
    /* 37^i */
    const static uint8_t factor[KEY_LEN] = {
        1, 37, 89, 221, 241, 213, 201, 13,
        225, 133, 57, 61, 209, 53, 169, 109,
        193, 229, 25, 157, 177, 149, 137, 205
    };
    uint8_t *vector = (uint8_t *) k.key;
    int i;
    /* GCC will vectorize it using AVX-256. */
    res = 0;
    for (i = 0; i < KEY_LEN; i++) {
        res += factor[i] * vector[i];
    }
#else
#if KEY_LEN != 8
#error "Unsupported key length!"
#endif
    res = *(unsigned long *) k.key;
#endif
    if (unlikely(res == 0xff)) {
        res = 0;
    }
    return res;
}

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destory,
				insert_func_t insert, update_func_t update, remove_func_t remove,
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_recover();

extern void index_layer_dump();

extern pkey_t bonsai_make_key(const void *key, size_t len);

#ifdef __cplusplus
}
#endif

#endif
