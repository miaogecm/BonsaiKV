#ifndef _BONSAI_H
#define _BONSAI_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <libpmemobj.h>

#include "atomic.h"
#include "thread.h"
#include "region.h"
#include "list.h"
#include "mtable.h"

typedef skiplist_t index_layer_t;

typedef void (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(pkey_t key, pval_t val);
typedef int (*remove_func_t)(pkey_t key);
typedef int (*lookup_func_t)(pkey_t key);
typedef int (*scan_func_t)(pkey_t key1, pkey_t key2);

struct index_layer {
	void *index_struct;

	insert_func_t insert;
	remove_func_t remove;
	lookup_func_t lookup;
	scan_func_t   scan;

	destory_func_t destory;
};

struct log_layer {
	unsigned int epoch;
	int pmem_fd[NUM_SOCKET];
	char* pmem_addr[NUM_SOCKET];
	struct log_region region[NUM_CPU];
	
	struct list_head mtable_list;

	insert_func_t insert;
	remove_func_t remove;
	lookup_func_t lookup;
};

struct data_layer {
	struct data_region region[NUM_SOCKET];

	struct list_head pnode_list;

	insert_func_t insert;
	remove_func_t remove;
	lookup_func_t lookup;
	scan_func_t   scan;
};

#define REGION_FPATH_LEN	19
#define OBJPOOL_FPATH_LEN	20

struct bonsai_desc {
	__le32 init;
	struct log_region_desc log_region[NUM_CPU];
	char log_region_fpath[NUM_SOCKET][REGION_FPATH_LEN];
};

struct bonsai_info {
	/* 1. bonsai info */
	int	fd;
	struct bonsai_desc	*desc;

	/* 2. layer info */
    struct index_layer 	i_layer;
    struct log_layer 	l_layer;
    struct data_layer 	d_layer;

	/* 3. thread info */	
	pthread_t 			tids[NUM_PFLUSH_THREAD];
	struct thread_info* pflushd[NUM_PFLUSH_THREAD];
};

#define INDEX(bonsai)    	(bonsai->i_layer)
#define LOG(bonsai)   		(bonsai->l_layer)
#define DATA(bonsai)  		(bonsai->d_layer)

extern struct bonsai_info *bonsai;

#endif
/*bonsai.h*/
