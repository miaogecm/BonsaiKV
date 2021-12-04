#ifndef __PNODE_H
#define __PNODE_H

#include <stdint.h>

#include "arch.h"
#include "pmem.h"
#include "hash.h"
#include "rwlock.h"
#include "list.h"
#include "cmp.h"
#include "numa.h"
#include "common.h"

#define NUM_ENT_PER_PNODE      		32
#define NUM_BUCKET      			8
#define NUM_ENT_PER_BUCKET     		4

#define BUCKET_IS_FULL(pnode, x) (find_unused_entry(pnode, \
    (pnode)->bitmap & ((1ULL << (x + BUCKET_ENT_NUM)) - (1ULL << x))) == -1)

#define ENTRY_ID(pnode, entry) ((entry - pnode - CACHELINE_SIZE) / sizeof(pentry_t))
#define PNODE_OF_ENT(ptr) ((struct pnode*)((char*) ptr - offsetof(struct pnode, entry)))

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
}__attribute__((aligned(CACHELINE_SIZE)));

extern int pnode_insert(struct pnode* pnode, struct numa_table* table, int numa_node, pkey_t key, pval_t value);
extern int pnode_remove(struct pnode* pnode, pkey_t key);

#endif
/*pnode.h*/
