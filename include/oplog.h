#ifndef __OPLOG_H
#define __OPLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "atomic.h"
#include "list.h"
#include "common.h"
#include "arch.h"

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
 * Every oplog is 35 bytes. We pack 7 oplogs in 4 cache lines,
 * which is called oplog block.
 */
struct oplog {
	__le8 		o_type; /* OP_INSERT or OP_REMOVE */
	__le8		o_numa_node; /* which numa node */
	__le8		o_flag; /* 1: pnode, 0: numa_table */
	__le64 		o_stamp; /* time stamp */
	__le64		o_addr; /* pnode or numa_table address */
	pentry_t 	o_kv; /* actual key-value pair */
}__packed;

/*
 * oplog_blk: 256 bytes
 */
struct oplog_blk {
	struct oplog logs[NUM_OPLOG_PER_BLK];
	__le8 flush; /* whether it has been flushed */
	__le8 cpu; /* which NUMA node */
	__le8 cnt; /* how many logs in oplog block */
	__le64 next; /* next oplog block */
}__packed;

#define OPLOG(val) (struct oplog*)((unsigned long)val - 27)

#define OPLOG_BLK_MASK	0xFFFFFFFFFFFFFF00
#define OPLOG_BLK(addr)	(struct oplog_blk*)((unsigned long)addr & OPLOG_BLK_MASK)

struct pnode;
struct mptable;
struct log_region;

extern struct oplog* alloc_oplog(struct log_region* region, pkey_t key, pval_t val, optype_t type, int cpu);

extern struct oplog* oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, 
			int cpu, struct mptable* mptable, struct pnode* pnode);

extern void oplog_flush();

#ifdef __cplusplus
}
#endif

#endif
