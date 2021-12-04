/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>

#include "pnode.h"
#include "oplog.h"
#include "common.h"
#include "arch.h"
#include "mtable.h"
#include "bonsai.h"

static uint64_t hash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

#define BUCKET_HASH(x) (hash(x) % BUCKET_NUM)

static int cmp(pkey_t a, pkey_t b) {
    if (a < b) return -1
    if (a > b) return 1;
    return 0;
}

static struct pnode* alloc_pnode() {
    TOID(struct pnode) toid;
    struct pnode* pnode;
	int i;

    POBJ_ALLOC(DATA(bonsai)->pop, &toid, struct pnode, sizeof(struct pnode));
#if 0
    /*i forget why ...*/
    POBJ_ZALLOC(pop[i], &toid, struct pnode, size + CACHELINE_SIZE);
    TOID_OFFSET(toid) = TOID_OFFSET(toid) + CACHELINE_SIZE - TOID_OFFSET(toid) % CACHELINE_SIZE;
#endif
    pnode = pmemobj_direct(toid);

	pnode->bitmap = cpu_to_le64(0);

    pnode->slot_lock = (rwlock_t*) malloc(sizeof(rwlock_t));
    rwlock_init(pnode->slot_lock);

    for (i = 0; i < BUCKET_NUM; i++) {
        pnode->bucket_lock[i] = (rwlock_t*) malloc(sizeof(rwlock_t));
        rwlock_init(pnode->bucket_lock[i]);
    }

	INIT_LIST_HEAD(&pnode->list);

	memset(pnode->slot, 0, PNODE_ENT_NUM + 1);

	pnode->table = NULL;

	memset(pnode->forward, 0, NUM_SOCKET * BUCKET_NUM * sizeof(__le64));

    pmemobj_persist(pop, pnode, sizeof(struct pnode));

    return pnode;
}

static void free_pnode(struct pnode* pnode) {
	PMEMoid toid = pmemobj_oid(top_node);
	int i;

	free(pnode->slot_lock);

	for (i = 0; i < BUCKET_NUM; i++)
		free(pnode->bucket_lock[i]);

	spin_lock(&DATA(bonsai)->lock);
	list_del(&pnode->list);
	spin_unlock(&DATA(bonsai)->lock);

	POBJ_FREE(&toid);
}

static void insert_pnode(struct pnode* pnode, pkey_t key) {
	struct pnode *pos, *prev;
	pkey_t max_key;
	
	spin_lock(&DATA(bonsai)->lock);
	list_for_each_entry(pos, &DATA(bonsai)->pnode_list, list) {
		max_key = pos->e[node->slot[0]];
		if (max_key > key)
			break;
		else
			prev = pos;
	}

	__list_add(&pnode->list, &prev->list, &pos->list);
	spin_unlock(&DATA(bonsai)->lock);
}

/*
 * find_unused_entry: return -1 if bitmap is full.
 */
static int find_unused_entry(uint64_t v) {
	int n = 1;    
	v = ~v;

    if (!v) return -1;
#if BUCKET_ENT_NUM >= 64
    if (!(v & 0xffffffff)) {v >>= 32; n += 32};
#endif
#if BUCKET_ENT_NUM >= 32
    if (!(v & 0xffff)) {v >>= 16; n += 16};
#endif
#if BUCKET_ENT_NUM >= 16
    if (!(v & 0xff)) {v >>= 8; n += 8};
#endif
#if BUCKET_ENT_NUM >= 8
    if (!(v & 0xf)) {v >>= 4; n += 4};
#endif
#if BUCKET_ENT_NUM >= 4
    if (!(v & 0x3)) {v >>= 2; n += 2};
#endif
#if BUCKET_ENT_NUM >= 2
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
    int pos, i;

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
 * pnode_insert: insert a kv-pair into a pnode
 * @pnode: persistent node
 * @table: NUMA mapping table
 * @numa_node: which numa node
 * @key: key
 * @val: value
 * return 0 if successful
 */
int pnode_insert(struct pnode* pnode, struct numa_table* table, int numa_node, pkey_t key, pval_t value) {
    int offset, pos, i, n, bucket_full = 0;
    uint64_t mask, bucket_id;
    uint64_t bitmap, removed;
    struct pnode* new_pnode;
    pkey_t max_key;

    bucket_id = BUCKET_HASH(key);
    offset = NUM_BUCKET * bucket_id;

	if (unlikely(!pnode)) {
		/* allocate a new pnode */
		pnode = alloc_pnode();
		table->pnode = pnode;
		pnode->table = table;
		insert_pnode(pnode, key);
	}

retry:
    write_lock(pnode->bucket_lock[bucket_id]);
    mask = (1ULL << (offset + NUM_BUCKET)) - (1ULL << offset);
    pos = find_unused_entry(pnode->bitmap & mask);
    if (pos != -1) {
        pentry_t entry = {key, value};

        pnode->entry[pos] = entry;

        mptable_update_addr(pnode->table, MPTABLE_NODE(table, numa_node), key, &pnode->entry[pos]);

        write_lock(pnode->slot_lock);
        sort_in_pnode(pnode, pos, key, OP_INSERT);
        write_unlock(pnode->slot_lock);

        write_unlock(pnode->bucket_lock[bucket_id]);
		
		bonsai_clflush(&pnode->bitmap, sizeof(__le64), 0);
		bonsai_clflush(&pnode->entry[pos], sizeof(pentry_t), 0);
		bonsai_clflush(&pnode->slot, PNODE_ENT_NUM + 1, 1);

        return 0;
    }
	write_unlock(pnode->bucket_lock[bucket_id]);

	/* split the node */
	bucket_full = 0;
    for (i = 0; i < NUM_BUCKET; i ++) {
        write_lock(pnode->bucket_lock[i]);
        if (BUCKET_IS_FULL(pnode, i)) {
            bucket_full = 1;
        }
    }

    if (bucket_full) {
        new_pnode = alloc_pnode();
        memcpy(new_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENT_PER_BUCKET);
        n = pnode->slot[0];
        bitmap = pnode->bitmap;
        removed = 0;

        for (i = n / 2 + 1; i <= n; i++) {
            removed |= 1ULL << pnode->slot[i];
            new_pnode->slot[i - n/2] = pnode->slot[i];
        }
        new_pnode->slot[0] = n - n / 2;
        new_pnode->bitmap = removed;

        pnode->bitmap &= ~removed;
        pnode->slot[0] = n/2;

		insert_pnode(new_pnode, pnode->e[pnode->slot[0]]);

		/* split the mapping table */
        mptable_split(pnode->mtable, new_pnode);
    }

    max_key = pnode->entry[slot[0]];
    for (i = NUM_BUCKET - 1; i >= 0; i --) 
        write_unlock(pnode->bucket_lock[i]);

	bonsai_clflush(&pnode->bitmap, sizeof(__le64), 0);
	bonsai_clflush(&pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 0);
	bonsai_clflush(&new_pnode->bitmap, sizeof(__le64), 0);
	bonsai_clflush(&new_pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 1);

    if (bucket_full)
        pnode = cmp(key, max_key) <= 0 ? pnode : new_pnode;

    goto retry;
}

/*
 * pnode_remove: remove an entry of a pnode
 * @pnode: the pnode
 * @key: key to be removed
 */
int pnode_remove(struct pnode* pnode, pkey_t key) {
    int bucket_id, offset, i;
	uint64_t mask;

    bucket_id = BUCKET_HASH(key);
    offset = NUM_ENT_PER_BUCKET * bucket_id;

	mask = (1ULL << (offset + NUM_ENT_PER_BUCKET)) - (1ULL << offset);

    write_lock(pnode->bucket_lock[bucket_id]);
	for (i = 0; i < NUM_ENT_PER_BUCKET; i ++)
		if (pnode->e[i].k == key)
			goto find;

	return -ENOENT;

find:
    mask |= (offset + i);
    pnode->bitmap &= mask;
	write_unlock(pnode->bucket_lock[bucket_id]);

    write_lock(pnode->slot_lock);
    sort_in_pnode(pnode, i, key, OP_REMOVE);
    write_unlock(pnode->slot_lock);

	if (unlikely(!pnode->slot[0])) {
		/* free this persistent node */
		free_pnode(pnode);
		numa_mptable_free(pnode->table);
		INDEX(bonsai)->remove(pnode->e[pnode->slot[0]]);
	} else {
		bonsai_clflush(&pnode->bitmap, sizeof(__le64), 0);
		bonsai_clflush(&pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 1);	
	}

    return 0;
}
/*
 * pnode_range: perform range query on [begin, end]
 * @pnode: pnode whose max_key is the upper bound of key1, search from its prev
 * @begin: begin_key
 * @end: end_key
 * @res_array: result_array
 */
int pnode_scan(struct pnode* pnode, pkey_t begin, pkey_t end, pentry_t* res_arr) {
    struct pnode* prev_pnode;
    int res_n = 0;
    uint8_t* slot;
    int i;

    pnode = PNODE_OF_ENT(pnode->forward[0][0]);
  
    prev_pnode = list_prev_entry(pnode, struct pnode);
    if (prev_pnode != NULL)
        pnode = prev_node;

    i = 0; slot = pnode->slot;
    while(i <= slot[0]) {
        if (cmp(pnode->entry[slot[i]], begin) >= 0) 
            break;
        i++;
    }

    while(pnode != NULL) {
        slot = pnode->slot;
        while(i <= slot[0]) {
            if (cmp(pnode->entry[slot[i]], end) > 0)
                return res_n;
            res_arr[res_n] = pnode->entry[slot[i]];
            res_n++; i++;
        }
        pnode = list_next_entry(pnode, struct pnode);
    }

    return res_n;
}

void free_pnode(struct pnode* pnode) {

    POBJ_FREE(pmemobj_oid(pnode));
}

int pnode_migrate(struct pnode* pnode, struct pentry* entry, int numa_id) {
    int bucket_id;
    struct pnode* local_pnode;
    int offset;
    int i;

    bucket_id = ENTRY_ID(pnode, entry) / BUCKET_ENT_NUM;
    if (pnode->forward[numa_id][bucket_id] == NULL) {
        local_pnode = alloc_pnode();
        if (!cmpxchg(&pnode->forward[numa_id][0], NULL, local_pnode->entry)) {
            free_pnode(local_pnode);
            return -EEXIST;
        }
        for (i = 1; i < BUCKET_NUM; i++) {
            pnode->forward[numa_id][i] = &local_pnode->entry[i * BUCKET_ENT_NUM];
        }
    }
    
    offset = bucket_id * BUCKET_ENT_NUM;
    memcpy(&local_pnode->entry[offset], &pnode->entry[offset], BUCKET_ENT_NUM * sizeof(pentry_t));
    clflush(&local_pnode->entry[offset], BUCKET_ENT_NUM * sizeof(pentry_t));

    return 0;
}
