#ifndef _BONSAI_H
#define _BONSAI_H

#ifdef __cplusplus
extern "C" {
#endif

#include <libpmemobj.h>
#include <thread.h>
#include <assert.h>

#include "atomic.h"
#include "region.h"
#include "list.h"
#include "common.h"
#include "spinlock.h"
#include "oplog.h"
#include "arch.h"
#include "rwlock.h"
#include "shim.h"
#include "cpu.h"

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, const void *key, size_t len, const void* value);
typedef int (*update_func_t)(void* index_struct, const void *key, size_t len, const void* value);
typedef int (*remove_func_t)(void* index_struct, const void *key, size_t len);
typedef void* (*lookup_func_t)(void* index_struct, const void *key, size_t len);
typedef int (*scan_func_t)(void* index_struct, const void *low, const void *high);

#define MPTABLE_ARENA_SIZE          (2 * 1024 * 1024 * 1024ul)  // 2GB

#define NUM_MERGE_HASH_BUCKET		131072

struct index_layer {
	void *index_struct, *pnode_index_struct;

	insert_func_t insert;
    update_func_t update;
	remove_func_t remove;
	lookup_func_t lookup;
	scan_func_t   scan;

	destory_func_t destory;
};

struct log_layer {
	unsigned int nflush; /* how many flushes */
	atomic_t exit; /* thread exit */
    atomic_t force_flush; /* force flush all logs */

    atomic_t epoch_passed;
	atomic_t checkpoint;

    struct {
        atomic_t cnt;
        char padding[CACHELINE_SIZE];
    } nlogs[NUM_CPU];
	
	int pmem_fd[NUM_SOCKET]; /* memory-mapped fd */
	char* pmem_addr[NUM_SOCKET]; /* memory-mapped address */

	struct log_region region[NUM_CPU]; /* per-cpu log region */

	pthread_barrier_t barrier; /* sorting barrier */
	struct list_head sort_list[NUM_PFLUSH_WORKER]; 

	struct hbucket buckets[NUM_MERGE_HASH_BUCKET]; /* hash table used in log flush */
};

struct data_layer {
	struct data_region region[NUM_SOCKET];
    struct pnode *sentinel;
};

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
	
	pthread_t 			tids[NUM_PFLUSH_THREAD + 1];
	struct thread_info* pflushd[NUM_PFLUSH_THREAD];
	struct thread_info* smo;
}____cacheline_aligned;

#define INDEX(bonsai)    	(&(bonsai->i_layer))
#define SHIM(bonsai)        (&(bonsai->s_layer))
#define LOG(bonsai)   		(&(bonsai->l_layer))
#define DATA(bonsai)  		(&(bonsai->d_layer))

struct long_key {
    char key[0];
};

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, struct pnode);
POBJ_LAYOUT_TOID(BONSAI, struct long_key);
POBJ_LAYOUT_END(BONSAI);

extern struct bonsai_info* bonsai;

pkey_t alloc_nvkey(pkey_t vkey);
void free_nvkey(pkey_t nvkey);

static inline const void *resolve_key(pkey_t key, uint64_t *aux, size_t *len) {
#ifndef LONG_KEY
    *aux = __builtin_bswap64(key);
	*len = 8;
	return aux;
#else
	struct data_layer *layer = DATA(bonsai);
    int node = get_numa_node(__this->t_cpu);
    *len = pkey_len(key);
    if (pkey_is_nv(key)) {
        return (const void *) layer->region[node].start + pkey_off(key);
    } else {
        return pkey_addr(key);
    }
#endif
}

int key_cmp(pkey_t a, pkey_t b, int pulled);

uint8_t pkey_get_signature(pkey_t key);
pkey_t pkey_prev(pkey_t key);

static inline int pkey_compare(pkey_t a, pkey_t b) {
	return key_cmp(a, b, 0);
}

static inline int pkey_compare_pulled(pkey_t a, pkey_t b) {
	return key_cmp(a, b, 1);
}

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destory,
				insert_func_t insert, update_func_t update, remove_func_t remove,
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_recover();

extern void index_layer_dump();

extern pkey_t bonsai_make_key(const void *key, size_t len);

#ifdef LONG_KEY
#define MK_K(k, l) (bonsai_make_key(k, l))
#endif

#ifdef __cplusplus
}
#endif

#endif
