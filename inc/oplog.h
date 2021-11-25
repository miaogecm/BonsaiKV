#ifndef __OPLOG_H
#define __OPLOG_H

#include "atomic.h"

typedef void (*index_func_t)(void*);

typedef enum {
	OP_INSERT = 0,
	OP_REMOVE,
	OP_UPDATE,
} optype_t;

/*
 * operation log definition in log layer
 */
struct oplog {
    atomic_t 			o_stamp;
	optype_t 			o_type;
	pentry_t 			o_kv;
	void*				o_addr;
    struct list_head 	o_list;
};

extern struct oplog* oplog_insert(pkey_t key, pval_t val, void *table, optype_t op);
extern int oplog_remove(pkey_t key, void *table);
pval_t oplog_lookup(pkey_t key, void *table);

#endif
