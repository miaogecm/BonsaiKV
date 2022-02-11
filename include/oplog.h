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
	struct list_head list;
	struct hlist_node node;
} merge_ent;

typedef enum {
	OP_NONE = -1,
	OP_INSERT = 0,
	OP_REMOVE,
} optype_t;

#define NUM_OPLOG_PER_BLK	9

/*
 * operation log definition in log layer
 * Every oplog is 26 bytes. We pack 9 oplogs in 4 cache lines,
 * which is called oplog block.
 */
struct oplog {
	__le8 		o_type; /* OP_INSERT or OP_REMOVE */
	__le8		o_numa_node; /* which numa node */
	__le64 		o_stamp; /* time stamp */
	pentry_t 	o_kv; /* actual key-value pair */
}__packed;

/*
 * oplog_blk: 256 bytes
 */
struct oplog_blk {
	struct oplog logs[NUM_OPLOG_PER_BLK];
	__le8 cpu; /* which NUMA node */
	__le8 cnt; /* how many logs in oplog block */
	__le64 epoch; /* epoch */
	__le64 next; /* next oplog block */
    char padding[4];
}__packed;

#define OPLOG(val) \
    (struct oplog*)((unsigned long)val - (unsigned long) &((struct oplog *) 0)->o_kv.v)

#define OPLOG_BLK_MASK	0xFFFFFFFFFFFFFF00
#define OPLOG_BLK(addr)	(struct oplog_blk*)((unsigned long)addr & OPLOG_BLK_MASK)

struct pnode;
struct mptable;
struct log_region;
struct log_layer;

extern struct oplog* alloc_oplog(struct log_region* region, int cpu);

extern struct oplog *oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, int cpu, struct pnode *pnode);

extern void oplog_flush();

extern struct oplog* log_layer_search_key(int cpu, pkey_t key);

extern void clean_pflush_buckets(struct log_layer* layer);

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

#ifdef __cplusplus
}
#endif

#endif
