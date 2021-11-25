#ifndef __OPLOG_H
#define __OPLOG_H

#include "atomic.h"

typedef void (*index_func_t)(void*);

typedef enum {
	OP_INSERT = 0,
	OP_REMOVE,
} optype_t;

/*
 * operation log definition in log layer
 */
struct oplog {
    atomic_t 			o_stamp;
	optype_t 			o_type;
	index_func_t 		o_func;
	pentry_t 			o_kv;	
    struct list_head 	o_list;
};

extern int oplog_insert(pkey_t key, pval_t val, void *table);
extern int oplog_remove(pkey_t key, void *table);
pval_t oplog_lookup(pkey_t key, void *table);

#endif
