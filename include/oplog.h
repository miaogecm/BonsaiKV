#ifndef __OPLOG_H
#define __OPLOG_H

#include "atomic.h"

#define MAX_HASH_BUCKET		64

typedef struct {
	struct oplog* log;
	struct hlist_node node;
} merge_ent;

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
	__le8		o_numa_node; /* which numa node */
	__le8		o_flag; /* 1: pnode, 0: numa_table */
	__le64 		o_stamp; /* time stamp */
	__le64		o_addr; /* pnode or numa_table addr */
	pentry_t 	o_kv; /* actual key-value pair */
};

struct oplog_blk {
	struct oplog[NUM_OPLOG_PER_BLK];
	char padding[1];
	__le8 cpu; /* which NUMA node */
	__le8 cnt; /* how many logs in oplog block */
	__le64 next; /* next oplog block */
};

#define OPLOG(val) (struct oplog*)((unsigned long)val - 26)

#define OPLOG_BLK_MASK	0x100
#define OPLOG_BLK(addr)	(struct oplog_blk*)(addr & OPLOG_BLK_MASK)

extern struct oplog* oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, struct mptable* mptable, struct pnode* node);

#endif
