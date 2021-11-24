#ifndef __PNODE_H
#define __PNODE_H

#include <stdint.h>

#include "arch.h"
#include "pmem.h"
#include "hash.h"
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

#define BUCKET_HASH(x) (hash(x) % BUCKET_NUM)

/*
 * persistent node definition in data layer
 */
struct pnode_s {
	/* first cache line */
    uint64_t* bitmap;
    uint64_t persist_bitmap;
	uint8_t slot[PNODE_ENT_NUM + 1];
    rwlock_t* slot_lock;
    rwlock_t* bucket_lock[BUCKET_NUM];
	struct list_head list;
	//char padding[8];

	/* second cache line */
    pentry_t entry[PNODE_ENT_NUM]__attribute__((aligned(CACHELINE_SIZE)));
}__attribute__((aligned(CACHELINE_SIZE)));

static inline struct pnode_s* alloc_presist_node() {
    TOID(struct pnode_s) toid;
    int size = sizeof(struct pnode_s);
    POBJ_ZALLOC(pop, &toid, struct pnode_s, size + CACHELINE_SIZE);
    TOID_OFFSET(toid) = TOID_OFFSET(toid) + CACHELINE_SIZE - TOID_OFFSET(toid) % CACHELINE_SIZE;

    struct pnode_s* pnode = pmemobj_direct(toid);
    pnode->slot_lock = (rwlock_t*) malloc(sizeof(rwlock_t));
    rwlock_init(pnode->slot_lock);
    int i;
    for (i = 0; i < BUCKET_NUM; i++) {
        pnode->bucket_lock[i] = (rwlock_t*) malloc(sizeof(rwlock_t));
        rwlock_init(pnode->bucket_lock[i]);
    }

    pmemobj_persist(pop, pnode, size);
    return pmemobj_direct(toid);
}

static inline int find_unused_entry(uint64_t v) {
    v = ~v;
    int n = 1;

    if (!v) return -1;
#if BUCKET_SIZE >= 64
    if (!(v & 0xffffffff)) {v >>= 32; n += 32};
#endif
#if BUCKET_SIZE >= 32
    if (!(v & 0xffff)) {v >>= 16; n += 16};
#endif
#if BUCKET_SIZE >= 16
    if (!(v & 0xff)) {v >>= 8; n += 8};
#endif
#if BUCKET_SIZE >= 8
    if (!(v & 0xf)) {v >>= 4; n += 4};
#endif
#if BUCKET_SIZE >= 4
    if (!(v & 0x3)) {v >>= 2; n += 2};
#endif
#if BUCKET_SIZE >= 2
    if (!(v & 0x1)) {v >>= 1; n += 1};
#endif

    n--;
    return n;
}

static inline int lower_bound(struct pnode_s* pnode, pkey_t key) {
    if (pnode == NULL)
        return 0;
    
    uint8_t* slot = pnode->slot;
    int l = 1, r = slot[0];
    while(l <= r) {
        int mid = l + (r - l) / 2;
        if (cmp(pnode->entry[slot[mid]].key, key) >= 0)
            r = mid - 1;
        else 
            l = mid + 1;
    }

    return l;
}

static inline void sort_in_pnode(struct pnode_s* pnode, int pos_e, pkey_t key, int op) {
    uint8_t* slot = pnode->slot;
    int pos = lower_bound(pnode, key);
    int i;
    if (op == 1) {
        for (i = slot[0]; i >= pos; i--)
            slot[i+1] = slot[i];
        slot[pos] = pos_e;
        slot[0]++;
    }
    else {
        for (i = pos; i < slot[0]; i++)
            slot[i] = slot[i+1];
        slot[0]--;
    }
}

int persist_node_insert(struct pnode_s* pnode, pkey_t key, char* value) {
    uint32_t bucket_id = BUCKET_HASH(key);
    int offset = BUCKET_SIZE * bucket_id;
    write_lock(pnode->bucket_lock[bucket_id]);
    uint64_t mask = (1ULL << (offset + BUCKET_SIZE)) - (1ULL << offset);
    int pos = find_unused_entry(pnode->bitmap & mask);
    if (pos != -1) {
        pentry_t entry = {key, value};
        pnode->entry[pos] = entry;
        write_lock(pnode->slot_lock);
        sort_in_pnode(pnode, pos, key, 1);
        write_unlock(pnode->slot_lock);
    }
}

#endif
/*pnode.h*/