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
 * if o_type == OP_INSERT
 * 		o_kv.key = key; o_kv.val = value; o_addr = address of pnode to be inserted
 * if o_type == OP_REMOVE
 * 		ONLY o_addr is available, o_addr = address of entry in pnode
 */
struct oplog {
    atomic_t 			o_stamp;
	optype_t 			o_type;
	index_func_t 		o_func;
	pentry_t 			o_kv;
	struct pnode*		o_node;
	void*				o_addr;
    struct list_head 	o_list;
};

extern struct oplog* oplog_insert(pkey_t key, pval_t val, void *table, optype_t op);

#endif
