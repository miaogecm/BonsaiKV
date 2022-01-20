#ifndef _BONSAI_H
#define _BONSAI_H

#ifdef __cplusplus
extern "C" {
#endif

#include <libpmemobj.h>
#include <thread.h>

#include "atomic.h"
#include "region.h"
#include "list.h"
#include "common.h"
#include "spinlock.h"
#include "hash_set.h"
#include "oplog.h"
#include "arch.h"
#include "rwlock.h"

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, pkey_t key, void* value);
typedef int (*update_func_t)(void* index_struct, pkey_t key, void* value);
typedef int (*remove_func_t)(void* index_struct, pkey_t key);
typedef void* (*lookup_func_t)(void* index_struct, pkey_t key);
typedef int (*scan_func_t)(void* index_struct, pkey_t low, pkey_t high);

#define MAX_HASH_BUCKET		64

struct index_layer {
	void *index_struct;

	insert_func_t insert;
	remove_func_t remove;
	lookup_func_t lookup;
	scan_func_t   scan;

	destory_func_t destory;
};

struct log_layer {
	unsigned int nflush; /* how many flushes */
	
	int pmem_fd[NUM_SOCKET]; /* memory-mapped fd */
	char* pmem_addr[NUM_SOCKET]; /* memory-mapped address */

	struct log_region region[NUM_CPU]; /* per-cpu log region */

	spinlock_t lock;
	struct list_head numa_table_list;

	struct hbucket buckets[MAX_HASH_BUCKET]; /* hash table used in log flush */
};

struct data_layer {
	struct data_region region[NUM_SOCKET];

	rwlock_t lock;
	struct list_head pnode_list;
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
    struct log_layer 	l_layer;
    struct data_layer 	d_layer;

	/* 3. thread info */
	struct thread_info* self;
	
	pthread_t 			tids[NUM_PFLUSH_THREAD];
	struct thread_info* pflushd[NUM_PFLUSH_THREAD];
}____cacheline_aligned;

#define INDEX(bonsai)    	(&(bonsai->i_layer))
#define LOG(bonsai)   		(&(bonsai->l_layer))
#define DATA(bonsai)  		(&(bonsai->d_layer))

extern int bonsai_init(char* index_name, init_func_t init, destory_func_t destroy,
				insert_func_t insert, remove_func_t remove, 
				lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_recover();

static int simple_hash(pkey_t key) {
	return (key % MAX_HASH_BUCKET);
}

#ifdef __cplusplus
}
#endif

#endif
