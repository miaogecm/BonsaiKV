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

/*
 * persistent node definition in data layer
 */
struct pnode {
	/* 1st cache line */
    __le64 				bitmap;
    rwlock_t* 			slot_lock;
    rwlock_t* 			bucket_lock[NUM_BUCKET];
	struct list_head 	list;

	/* 2rd - 10th cache line */
    pentry_t 			e[NUM_ENT_PER_PNODE];

	/* 11th cache line */
    __le8 				slot[NUM_ENT_PER_PNODE + 1];
	struct numa_table* 	table;
	char				padding[23];

	/* 12th cache line */	
	__le64 				forward[NUM_SOCKET][NUM_BUCKET];	
}____cacheline_aligned;

static int key_cmp(pkey_t a, pkey_t b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

extern int pnode_insert(struct pnode* pnode, struct numa_table* table, int numa_node, pkey_t key, pval_t value);
extern int pnode_remove(struct pnode* pnode, pkey_t key);
extern pval_t* pnode_lookup(struct pnode* pnode, pkey_t key);

extern pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node);

extern int pnode_scan(struct pnode* pnode, pkey_t begin, pkey_t end, pentry_t* res_arr);

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
