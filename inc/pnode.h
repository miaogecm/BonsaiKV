#ifndef __PNODE_H
#define __PNODE_H

#include <stdint.h>

#include "arch.h"
#include "pmem.h"
#include "hash.h"
#include "rwlock.h"
#include "list.h"
#include "cmp.h"

typedef uint64_t pkey_t;

typedef struct pentry {
    pkey_t key;
    char* value;
} pentry_t;

#define GET_NODE(ptr) ((pentry_t*)ptr)
#define GET_KEY(ptr) (GET_NODE(ptr)->key)
#define GET_VALUE(ptr) (GET_NODE(ptr)->value)

#define PNODE_ENT_NUM       	32
#define BUFFER_SIZE_MUL 		2
#define BUCKET_SIZE     		8
#define BUCKET_NUM      		4

#define BUCKET_HASH(x) (phash(x) % BUCKET_NUM)

#define BUCKET_IS_FULL(pnode, x) (find_unused_entry(pnode, \
    (pnode)->bitmap & ((1ULL << (x + BUCKET_SIZE)) - (1ULL << x))) == -1)

#define ENTRY_ID(pnode, entry) ((entry - pnode - CACHELINE_SIZE) / sizeof(pentry_t))
/*
 * persistent node definition in data layer
 */
struct pnode_struct {
	/* first cache line */
    uint64_t* bitmap;
    uint64_t persist_bitmap;
    rwlock_t* slot_lock;
    rwlock_t* bucket_lock[BUCKET_NUM];
	char padding[40 - 8 * BUCKET_NUM];

	/* second cache line */
    pentry_t entry[PNODE_ENT_NUM]__attribute__((aligned(CACHELINE_SIZE)));

    uint8_t slot[PNODE_ENT_NUM + 1];
    struct list_head list;
}__attribute__((aligned(CACHELINE_SIZE)));

extern int pnode_insert(struct pnode* pnode, pkey_t key, char* value);
extern int pnode_remove(struct pnode* pnode, pentry_t* pentry);

#endif
/*pnode.h*/
