#ifndef __OPLOG_H
#define __OPLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "atomic.h"
#include "list.h"
#include "common.h"
#include "arch.h"

typedef enum {
	OP_NONE = -1,
	OP_INSERT = 0,
	OP_REMOVE,
} optype_t;

/*
 * operation log definition in log layer
 * For integer keys, each oplog is 23 bytes. We pack 10 oplogs in 4 cache lines (rest: 26),
 * For string keys, each oplog is 47 bytes. We pack 5 oplogs in 4 cache lines (rest: 21),
 * which is called oplog block.
 */
struct oplog {
    pentry_t    o_kv;    /* key-value pair */
	__le64 		o_stamp; /* time stamp */
	__le8 		o_type;  /* OP_INSERT or OP_REMOVE */
}__packed;

#ifdef STR_KEY
#define NUM_OPLOG_PER_BLK	5
#else
#define NUM_OPLOG_PER_BLK	10
#endif

/*
 * oplog_blk: 256 bytes
 */
struct oplog_blk {
	struct oplog logs[NUM_OPLOG_PER_BLK];
	__le8 cpu; /* which CPU */
	__le8 cnt; /* how many logs in oplog block */
	__le64 epoch; /* epoch */
	__le64 next; /* next oplog block */
#ifdef STR_KEY
    char padding[3];
#else
    char padding[8];
#endif
}__packed;

#define OPLOG(val)      (struct oplog*)(val)

#define OPLOG_BLK_MASK	0xFFFFFFFFFFFFFF00
#define OPLOG_BLK(addr)	(struct oplog_blk*)((unsigned long)addr & OPLOG_BLK_MASK)

struct pnode;
struct mptable;
struct log_region;
struct log_layer;

extern struct oplog* alloc_oplog(struct log_region* region, int cpu);

extern struct oplog *oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, int cpu);

extern void oplog_flush();

extern struct oplog* log_layer_search_key(int cpu, pkey_t key);

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

#ifdef __cplusplus
}
#endif

#endif
