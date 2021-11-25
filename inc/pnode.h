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
struct pnode_s {
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

static inline int pnode_insert(struct pnode_s* pnode, pkey_t key, char* value) {
    uint32_t bucket_id = BUCKET_HASH(key);
    int offset = BUCKET_SIZE * bucket_id;
retry:;
    write_lock(pnode->bucket_lock[bucket_id]);
    uint64_t mask = (1ULL << (offset + BUCKET_SIZE)) - (1ULL << offset);
    int pos = find_unused_entry(pnode->bitmap & mask);
    if (pos != -1) {
        pentry_t entry = {key, value};
        pnode->entry[pos] = entry;
        write_lock(pnode->slot_lock);
        sort_in_pnode(pnode, pos, key, 1);
        write_unlock(pnode->slot_lock);
        write_unlock(pnode->bucket_lock[bucket_id]);
        return 1;
    }

    write_unlock(pnode->bucket_lock[bucket_id]);
    int bucket_full_flag = 0;
    int i;
    for (i = 0; i < BUCKET_NUM; i++) {
        write_lock(pnode->bucket_lock[i]);
        if (BUCKET_IS_FULL(pnode, i)) {
            bucket_full_flag = 1;
        }
    }
    struct pnode_s* new_pnode;
    if (bucket_full_flag) {
        struct pnode_s* = alloc_presist_node();
        memcpy(new_pnode->entry, pnode->entry, sizeof(pentry_t) * PNODE_ENT_NUM);
        int n = pnode->slot[0];
        uint64_t bitmap = pnode->bitmap;
        uint64_t removed = 0;
        int i;
        for (i = n / 2 + 1; i <= n; i++) {
            removed |= 1ULL << pnode->slot[i];
            new_pnode->slot[i - n/2] = pnode->slot[i];
        }
        new_pnode->slot[0] = n - n / 2;
        new_pnode->bitmap = removed;

        pnode->bitmap &= ~removed;
        pnode->slot[0] = n/2;

        list_add(new_pnode, pnode);
    }

    pkey_t max_key = pnode->entry[slot[0]];
    for (i = 0; i < BUCKET_NUM; i++) {
        write_unlock(pnode->bucket_lock[i]);
    }
    if (bucket_full_flag) {
        pnode = cmp(key, max_key) <= 0 ? pnode : new_pnode;
    }
    goto retry;
}

static int inline pnode_remove(struct pnode_s* pnode, pentry_t* pentry) {
    int pos = ENTRY_ID(pnode, pentry);
    uint64_t mask = ~(1ULL << pos);
    pnode->bitmap &= mask;
    pnode->persist_bitmap &= mask;
    return 1;
}

#endif
/*pnode.h*/