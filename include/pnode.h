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

#define PNODE_ENT_NUM       	32
#define BUCKET_ENT_NUM     		8
#define BUCKET_NUM      		4

#define BUCKET_HASH(x) (phash(x) % BUCKET_NUM)

#define BUCKET_IS_FULL(pnode, x) (find_unused_entry(pnode, \
    (pnode)->bitmap & ((1ULL << (x + BUCKET_ENT_NUM)) - (1ULL << x))) == -1)

#define ENTRY_ID(pnode, entry) ((entry - pnode - CACHELINE_SIZE) / sizeof(pentry_t))
#define PNODE_OF_ENT(ptr) ((struct pnode*)((char*) ptr - offsetof(struct pnode, entry)))

#define IS_IN_NVM(ptr) (pmemobj_oid(ptr) != OID_NULL)

/*
 * persistent node definition in data layer
 */
struct pnode {
	/* first cache line */
    uint64_t* v_bitmap;
    uint64_t p_bitmap;
    rwlock_t* slot_lock;
    rwlock_t* bucket_lock[BUCKET_NUM];
	struct mtable* mtable;

	/* second cache line */
    pentry_t entry[PNODE_ENT_NUM]__attribute__((aligned(CACHELINE_SIZE)));

    uint8_t slot[PNODE_ENT_NUM + 1];
	void *forward[NUM_SOCKET][BUCKET_NUM];
    struct list_head list;
}__attribute__((aligned(CACHELINE_SIZE)));

extern int pnode_insert(struct pnode* pnode, pkey_t key, pval_t value);
extern int pnode_remove(struct pnode* pnode, pentry_t* pentry);
extern void commit_bitmap(struct pnode* pnode, int pos, optype_t op);

#endif
/*pnode.h*/
