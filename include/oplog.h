#ifndef __OPLOG_H
#define __OPLOG_H

#include "atomic.h"

typedef enum {
	OP_INSERT = 0,
	OP_REMOVE,
} optype_t;

#define NUM_OPLOG_PER_BLK	7

/*
 * operation log definition in log layer
 * Every oplog is 34 bytes. We pack seven oplogs in four cache
 * lines, which is called oplog block.
 */
struct oplog {
	__le8 		o_type; /* OP_INSERT or OP_REMOVE */
	char		o_pad[1]; /* padding */
	__le64 		o_stamp; /* time stamp */
	__le64		o_node; /* which pnode belongs to */
	pentry_t 	o_kv; /* actual key-value pair */
};

struct oplog_blk {
	struct oplog[NUM_OPLOG_PER_BLK];
	__le16 padding;
	__le64 cnt; /* how many logs in oplog block */
	__le64 next; /* next oplog block */
};

extern struct oplog* oplog_insert(pkey_t key, pval_t val, void *table, optype_t op);

#endif
