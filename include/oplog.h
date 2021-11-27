#ifndef __OPLOG_H
#define __OPLOG_H

#include "atomic.h"

typedef enum {
	OP_INSERT = 0,
	OP_REMOVE,
} optype_t;

/*
 * operation log definition in log layer
 */
struct oplog {
	__le8 		o_type; /* OP_INSERT or OP_REMOVE */
	__le64 		o_stamp; /* time stamp */
	__le64		o_node; /* which pnode belongs to */
	pentry_t 	o_kv; /* actual key-value pair */
};

extern struct oplog* oplog_insert(pkey_t key, pval_t val, void *table, optype_t op);

#endif
