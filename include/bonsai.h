#ifndef _BONSAI_H
#define _BONSAI_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <libpmemobj.h>

#include "atomic.h"
#include "thread.h"
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
	spinlock_t locks[NUM_CPU];
	struct list_head log_list[NUM_CPU];
	struct list_head mtable_list;

	insert_func_t insert;
	remove_func_t remove;
	lookup_func_t lookup;
};

struct data_layer {
	struct list_head pnode_list;

	insert_func_t insert;
	remove_func_t remove;
	lookup_func_t lookup;
	scan_func_t   scan;
};

struct bonsai {
    uint8_t 			bonsai_init;
	int					total_cpu;
	
	/* 1. thread info */	
	pthread_t 			tids[NUM_PFLUSH_THREAD];
	struct thread_info* pflushd[NUM_PFLUSH_THREAD];

	/* 2. layer info */
    struct index_layer 	i_layer;
    struct log_layer 	l_layer;
    struct data_layer 	d_layer;
};

#define INDEX(bonsai)    	(bonsai->i_layer)
#define LOG(bonsai)   		(bonsai->l_layer)
#define DATA(bonsai)  		(bonsai->d_layer)

extern struct bonsai *bonsai;

#endif
/*bonsai.h*/
