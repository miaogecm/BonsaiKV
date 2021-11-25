/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */

#include "pnode.h"
#include "oplog.h"
#include "common.h"
#include "arch.h"
#include "mtable.h"

static struct pnode* alloc_pnode() {
    TOID(struct pnode) toid;
    int size;
    struct pnode* pnode;
    int i;

    size = sizeof(struct pnode);
    POBJ_ZALLOC(pop, &toid, struct pnode, size + CACHELINE_SIZE);
    TOID_OFFSET(toid) = TOID_OFFSET(toid) + CACHELINE_SIZE - TOID_OFFSET(toid) % CACHELINE_SIZE;

    pnode = pmemobj_direct(toid);
    pnode->slot_lock = (rwlock_t*) malloc(sizeof(rwlock_t));
    rwlock_init(pnode->slot_lock);

    for (i = 0; i < BUCKET_NUM; i++) {
        pnode->bucket_lock[i] = (rwlock_t*) malloc(sizeof(rwlock_t));
        rwlock_init(pnode->bucket_lock[i]);
    }

    pmemobj_persist(pop, pnode, size);
    return pnode;
}

static int find_unused_entry(uint64_t v) {
	int n = 1;    
	v = ~v;

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

static int lower_bound(struct pnode* pnode, pkey_t key) {
    uint8_t* slot;
    int l, r;

    if (pnode == NULL) 
        return 0;
    
    slot = pnode->slot;
    l = 1;
    r = slot[0];

    while(l <= r) {
        int mid;

        mid = l + (r - l) / 2;
        if (cmp(pnode->entry[slot[mid]].key, key) >= 0)
            r = mid - 1;
        else 
            l = mid + 1;
    }

    return l;
}

static void sort_in_pnode(struct pnode* pnode, int pos_e, pkey_t key, optype_t op) {
    uint8_t* slot;
    int pos;
    int i;

    slot = pnode->slot;
    pos = lower_bound(pnode, key);

    if (op == OP_INSERT) {
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

/*
 * pnode_insert: insert an kv-pair into a pnode
 */
int pnode_insert(struct pnode* pnode, pkey_t key, pval_t value) {
    uint32_t bucket_id;
    int offset, pos;
    uint64_t mask;
    int bucket_full_flag;
    struct pnode* new_pnode;
    pkey_t max_key;
    int i;

    bucket_id = BUCKET_HASH(key);
    offset = BUCKET_SIZE * bucket_id;

retry:

    write_lock(pnode->bucket_lock[bucket_id]);
    mask = (1ULL << (offset + BUCKET_SIZE)) - (1ULL << offset);
    pos = find_unused_entry(pnode->v_bitmap & mask);
    if (pos != -1) {
        pentry_t entry;

        entry = {key, value};
        pnode->entry[pos] = entry;
        clflush(&pnode->entry[pos], sizeof(pentry_t));
        mfence();

        write_lock(pnode->slot_lock);
        sort_in_pnode(pnode, pos, key, OP_INSERT);
        write_unlock(pnode->slot_lock);

        write_unlock(pnode->bucket_lock[bucket_id]);
        return 1;
    }

    write_unlock(pnode->bucket_lock[bucket_id]);

    bucket_full_flag = 0;
    for (i = 0; i < BUCKET_NUM; i++) {
        write_lock(pnode->bucket_lock[i]);
        if (BUCKET_IS_FULL(pnode, i)) {
            bucket_full_flag = 1;
        }
    }

    if (bucket_full_flag) {
        uint64_t bitmap, removed;
		int n;

        new_pnode* = alloc_presist_node();
        memcpy(new_pnode->entry, pnode->entry, sizeof(pentry_t) * PNODE_ENT_NUM);
        n = pnode->slot[0];
        bitmap = pnode->v_bitmap;
        removed = 0;

        for (i = n / 2 + 1; i <= n; i++) {
            removed |= 1ULL << pnode->slot[i];
            new_pnode->slot[i - n/2] = pnode->slot[i];
        }
        new_pnode->slot[0] = n - n / 2;
        new_pnode->v_bitmap = removed;

        pnode->v_bitmap &= ~removed;
        pnode->slot[0] = n/2;

        list_add(new_pnode, pnode);

        mtable_split(pnode->mtable, new_pnode);
    }

    max_key = pnode->entry[slot[0]];
    for (i = 0; i < BUCKET_NUM; i++) {
        write_unlock(pnode->bucket_lock[i]);
    }
    if (bucket_full_flag) {
        pnode = cmp(key, max_key) <= 0 ? pnode : new_pnode;
    }
    goto retry;
}

/*
 * pnode_remove: remove an entry of a pnode
 * @pnode: the pnode
 * @pentry: the address of entry to be removed
 */
int pnode_remove(struct pnode* pnode, pentry_t* pentry) {
    uint64_t mask;
    pkey_t key;
	int pos;

    pos = ENTRY_ID(pnode, pentry);
    key = pnode->entry[pos];
    mask = ~(1ULL << pos);
    pnode->v_bitmap &= mask;

    write_lock(pnode->slot_lock);
    sort_in_pnode(pnode, pos, key, OP_REMOVE);
    write_unlock(pnode->slot_lock);

    return 1;
}
/*
 * pnode_range: range query from key1 to key2, return the number of entries whose value:[key1, key2]
 * @pnode: pnode whose max_key is the upper bound of key1, search from its prev
 * @key1: begin_key
 * @key2: end_key
 * @res_array: result_array
 */
int pnode_range(struct pnode* pnode, pkey_t key1, pkey_t key2, pentry_t* res_arr) {
    struct pnode* prev_pnode;
    int res_n = 0;
    uint8_t* slot;
    int i;

    prev_pnode = list_prev_entry(pnode, struct pnode);
    if (prev_pnode != NULL)
        pnode = prev_node;

    i = 0;
    slot = pnode->slot;
    while(i <= slot[0]) {
        if (cmp(pnode->entry[slot[i]], key1) >= 0) {
            break;
        }
        i++;
    }
    while(pnode != NULL) {
        slot = pnode->slot;
        while(i <= slot[0]) {
            if (cmp(pnode->entry[slot[i]], key2) > 0) {
                return res_n;
            }
            res_arr[res_n] = pnode->entry[slot[i]];
            res_n++;
            i++;
        }
        pnode = list_next_entry(pnode, struct pnode);
    }

    return res_n;
}

/*
 * pnode_persist: persist the p_bitmap of pnode after a operation
 * @pnode: pnode
 * @pos: the position where insert/remove an entry
 * @op: insert/remove
 */
void commit_bitmap(struct pnode* pnode, int pos, optype_t op) {
    uint64_t mask;
    
    mask = 1ULL << pos;
    if (op == OP_INSERT) {
        pnode->p_bitmap |= mask;
    } else {
        pnode->p_bitmap &= ~mask;
    }

    clflush(pnode->p_bitmap, sizeof(uint64_t));
    mfence();
}

void free_pnode(struct pnode* pnode) {

}
