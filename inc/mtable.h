#ifndef _CUCKOO_HASH
#define _CUCKOO_HASH

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <assert.h>

#include "common.h"
#include "spinlock.h"
#include "hash.h"

typedef struct mtable_ent {
    uint8_t used;
    rwlock_t* lock;
    void* addr;
} mtable_ent_t;

struct mtable {
    uint32_t total_ent; 
    uint32_t used_ent;
	rwlock_t* lock;
    int64_t seeds[3];
    struct pnode* pnode;
	struct list_head list;
	mtable_ent_t e[0];
};

extern int cuckoo_insert(cuckoo_hash_t* cuckoo, cuckoo_hash_t key, size_t value);

#endif
/*cuckoo_hash.h*/
