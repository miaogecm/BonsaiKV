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
	
	pthread_t 			tids[NUM_PFLUSH_THREAD];
	struct thread_info* pflushd[NUM_PFLUSH_THREAD];
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

#define OFF_CHECK_MASK	0xffff000000000000

#ifdef LONG_KEY

pkey_t alloc_nvkey(pkey_t vkey);
void free_nvkey(pkey_t nvkey);

#endif

/* Convert bonsai key to normal string key (used in i_layer). */
static inline const void *resolve_key(pkey_t key, uint64_t *aux, uint16_t* len) {
#ifndef LONG_KEY
    *aux = __builtin_bswap64(key);
	*len = 8;
	return aux;
#else
	struct data_layer* layer = DATA(bonsai);
	*len = pkey_len(key);
	return (const void *) layer->key_start + PKEY_OFF(key);
#endif
}

static int key_cmp(pkey_t a, pkey_t b) {
#ifndef LONG_KEY
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
#else
    struct data_layer* layer = DATA(bonsai);
    char* start = layer->key_start;
	uint16_t a_len, b_len;

	char* srca = resolve_key(&a, &a_len);
	char* srcb = resolve_key(&b, &b_len);

    char sa[a_len];
    char sb[b_len];
    memcpy(sa, srca, a_len);
    memcpy(sb, srcb, b_len);

    return strcmp(sa, sb);
#endif
}

static inline uint8_t pkey_get_signature(pkey_t key) {
#ifndef LONG_KEY
    return key;
#else
	return 1;
#endif
}

static inline pkey_t pkey_prev(pkey_t key) {
#ifndef LONG_KEY
    return key - 1;
#else
	struct data_layer* layer = DATA(bonsai);
    char* start = layer->key_start;
	char* dest = start + ((key >> KEY_LEN_BITS) << KEY_LEN_BITS);
	uint16_t len = (uint16_t) ((key >> KEY_OFF_BITS) << KEY_OFF_BITS);
	char* prev = (char*) malloc(len);
	int i;

	memcpy(prev, dest, len);

	i = len - 1;
	while(1) {
		if (prev[i]) {
			prev[i]--;
			break;
		}
		i--;
		assert(i >= 0);
	}

	return prev;
#endif
}

static inline int pkey_compare(pkey_t a, pkey_t b) {
	return key_cmp(a, b);
}

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destory,
				insert_func_t insert, update_func_t update, remove_func_t remove,
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_recover();

extern void index_layer_dump();

#ifdef __cplusplus
}
#endif

#endif
