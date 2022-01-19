#ifndef __PNODE_H
#define __PNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "arch.h"
#include "rwlock.h"
#include "list.h"
#include "common.h"
#include "numa_config.h"

#define NUM_ENT_PER_PNODE      		32
#define NUM_BUCKET      			8
#define NUM_ENT_PER_BUCKET     		4
#define PNODE_BITMAP_FULL			(~(1UL<<64))

enum {
	PNODE_DATA_CLEAN = 0,
	PNODE_DATA_STALE,
};

/*
 * persistent node definition in data layer
 */
struct pnode {
	/* 1st cache line */
	__le8				stale; /* indicate wether this pnode data is stale */
	char				padding1[7];
    __le64 				bitmap;
    rwlock_t* 			slot_lock;
    struct numa_table* 	table;
	struct list_head 	list;

	/* 2rd - 10th cache line */
    pentry_t 			e[NUM_ENT_PER_PNODE];

	/* 11th cache line */
    __le8 				slot[NUM_ENT_PER_PNODE + 1]; /* slot[0]: how many entries */
	char				padding2[31];

	/* 12th cache line */
	rwlock_t* 			bucket_lock[NUM_BUCKET];

	/* 13th cache line */	
	__le64 				forward[NUM_SOCKET][NUM_BUCKET];	
}__packed;

static int key_cmp(pkey_t a, pkey_t b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

extern int pnode_insert(struct pnode* pnode, struct numa_table* table, int numa_node, pkey_t key, pval_t value);
extern int pnode_remove(struct pnode* pnode, pkey_t key);
extern pval_t* pnode_lookup(struct pnode* pnode, pkey_t key);

extern pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node);

extern int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr);

extern void print_pnode(struct pnode* pnode);
extern void dump_pnodes();

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
