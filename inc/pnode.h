#ifndef __PNODE_H
#define __PNODE_H

#include "rwlock.h"

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

/*
 * persistent node definition in data layer
 */
struct pnode {
	/* first cache line */
    uint64_t bitmap;
	uint64_t slot[PNODE_SIZE + 1];
    rwlock_t* slot_lock;
    rwlock_t* bucket_lock[BUCKET_NUM];
	struct list_head list;
	char padding[8];

	/* second cache line */
    pentry_t entry[PNODE_ENT_NUM] __attribute__((aligned(CACHELINE_SIZE)));
};

#endif
