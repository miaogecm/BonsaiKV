/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>

#include "pnode.h"
#include "oplog.h"
#include "mptable.h"
#include "bonsai.h"
#include "cpu.h"
#include "arch.h"
#include "bitmap.h"

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, struct pnode);
POBJ_LAYOUT_END(BONSAI);

extern struct bonsai_info *bonsai;

static uint64_t hash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

#define BUCKET_HASH(x) (hash(x) % NUM_BUCKET)

static struct pnode* alloc_pnode(int node) {
	struct data_layer *layer = DATA(bonsai);
    TOID(struct pnode) toid;
    struct pnode* pnode;
	PMEMobjpool* pop;
	int i;

	pop = layer->region[node].pop;
    POBJ_ALLOC(pop, &toid, struct pnode, sizeof(struct pnode), NULL, NULL);
    // TODO 
    // check if it is 64-alian
#if 0
    /*i forget why ...*/
    POBJ_ZALLOC(pop[i], &toid, struct pnode, size + CACHELINE_SIZE);
    TOID_OFFSET(toid) = TOID_OFFSET(toid) + CACHELINE_SIZE - TOID_OFFSET(toid) % CACHELINE_SIZE;
#endif
    pnode = pmemobj_direct(toid.oid);

	pnode->bitmap = cpu_to_le64(0);

    pnode->slot_lock = (rwlock_t*) malloc(sizeof(rwlock_t));
    rwlock_init(pnode->slot_lock);

    for (i = 0; i < NUM_BUCKET; i++) {
        pnode->bucket_lock[i] = (rwlock_t*) malloc(sizeof(rwlock_t));
        rwlock_init(pnode->bucket_lock[i]);
    }

	INIT_LIST_HEAD(&pnode->list);

	memset(pnode->slot, 0, NUM_ENT_PER_PNODE + 1);

	pnode->table = NULL;

	memset(pnode->forward, 0, NUM_SOCKET * NUM_BUCKET * sizeof(__le64));

    pmemobj_persist(pop, pnode, sizeof(struct pnode));

    return pnode;
}

static void free_pnode(struct pnode* pnode) {
	PMEMoid toid = pmemobj_oid(pnode);
	int i;

	free(pnode->slot_lock);

	for (i = 0; i < NUM_BUCKET; i++)
		free(pnode->bucket_lock[i]);

	spin_lock(&DATA(bonsai)->lock);
	list_del(&pnode->list);
	spin_unlock(&DATA(bonsai)->lock);

	POBJ_FREE(&toid);
}

static void insert_pnode(struct pnode* pnode, pkey_t key) {
	struct pnode *pos, *prev = NULL;
	struct data_layer* layer = DATA(bonsai);
	struct list_head* head = &layer->pnode_list;
	pkey_t max_key;

	spin_lock(&layer->lock);
	list_for_each_entry(pos, head, list) {
		max_key = pos->e[pnode->slot[0]].k;
		if (key_cmp(max_key, key))
			break;
		else
			prev = pos;
	}

	if (!prev)
		list_add(&pnode->list, head);
	else
		__list_add(&pnode->list, &prev->list, &pos->list);
	spin_unlock(&layer->lock);
}

/*
 * find_unused_entry: return -1 if bucket is full.
 */
static int find_unused_entry(pkey_t key, uint64_t bitmap, int bucket_id) {
	uint64_t mask, pos;
	int offset;

	bucket_id = BUCKET_HASH(key);
	offset = NUM_ENT_PER_BUCKET * bucket_id;
	mask = (1UL << (offset + NUM_ENT_PER_BUCKET)) - (1UL << offset);
	bitmap = (bitmap & mask) | (~mask);
	
	if (bitmap == NUM_ENT_PER_PNODE)
		return -1;
	
	pos = find_first_zero_bit(&bitmap, NUM_ENT_PER_PNODE);

    return pos;
}

static int bucket_is_full(struct pnode* pnode, int bucket) {
	return 1;
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
        if (key_cmp(pnode->e[slot[mid]].k, key) >= 0)
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
    int bucket_id, pos, i, n, bucket_full = 0;
    uint64_t removed;
    struct pnode* new_pnode;
    pkey_t max_key;

	kv_debug("pnode_insert: pnode %016lx table %016lx <%lu %lu>\n", pnode, table, key, value);
	
	if (unlikely(!pnode)) {
		/* allocate a new pnode */
		pnode = alloc_pnode(numa_node);
		table->pnode = pnode;
		pnode->table = table;
		insert_pnode(pnode, key);
	}

retry:
	bucket_id = BUCKET_HASH(key);
    write_lock(pnode->bucket_lock[bucket_id]);
    pos = find_unused_entry(key, pnode->bitmap, bucket_id);
    if (pos != -1) {
		/* bucket is not full */
        pentry_t entry = {key, value};
        pnode->e[pos] = entry;

        mptable_update_addr(pnode->table, numa_node, key, &pnode->e[pos].v);

        write_lock(pnode->slot_lock);
        sort_in_pnode(pnode, pos, key, OP_INSERT);
        write_unlock(pnode->slot_lock);

		set_bit(pos, &pnode->bitmap);
        write_unlock(pnode->bucket_lock[bucket_id]);
		
		bonsai_flush(&pnode->bitmap, sizeof(__le64), 0);
		bonsai_flush(&pnode->e[pos], sizeof(pentry_t), 0);
		bonsai_flush(&pnode->slot, NUM_ENT_PER_PNODE + 1, 1);

        return 0;
    }
	write_unlock(pnode->bucket_lock[bucket_id]);

	/* split the node */
	bucket_full = 0;
    for (i = 0; i < NUM_BUCKET; i ++) {
        write_lock(pnode->bucket_lock[i]);
        if (bucket_is_full(pnode, i)) {
            bucket_full = 1;
        }
    }

    if (bucket_full) {
        new_pnode = alloc_pnode(numa_node);
        memcpy(new_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENT_PER_BUCKET);
        n = pnode->slot[0];
        removed = 0;

        for (i = n / 2 + 1; i <= n; i++) {
            removed |= 1ULL << pnode->slot[i];
            new_pnode->slot[i - n/2] = pnode->slot[i];
        }
        new_pnode->slot[0] = n - n / 2;
        new_pnode->bitmap = removed;

        pnode->bitmap &= ~removed;
        pnode->slot[0] = n/2;

		insert_pnode(new_pnode, pnode->e[pnode->slot[0]].k);

		/* split the mapping table */
        mptable_split(pnode->table, new_pnode);
    }

    max_key = pnode->e[pnode->slot[0]].k;
    for (i = NUM_BUCKET - 1; i >= 0; i --) 
        write_unlock(pnode->bucket_lock[i]);

	bonsai_flush(&pnode->bitmap, sizeof(__le64), 0);
	bonsai_flush(&pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 0);
	bonsai_flush(&new_pnode->bitmap, sizeof(__le64), 0);
	bonsai_flush(&new_pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 1);

    if (bucket_full)
        pnode = key_cmp(key, max_key) <= 0 ? pnode : new_pnode;

    goto retry;
}

/*
 * pnode_remove: remove an entry of a pnode
 * @pnode: the pnode
 * @key: key to be removed
 */
int pnode_remove(struct pnode* pnode, pkey_t key) {
	struct index_layer *i_layer = INDEX(bonsai);
    int bucket_id, offset, i;
	uint64_t mask;

	kv_debug("pnode_remove: pnode %016lx <%lu>\n", pnode, key);

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
		i_layer->remove(i_layer->index_struct, pnode->e[pnode->slot[0]].k);
	} else {
		bonsai_flush(&pnode->bitmap, sizeof(__le64), 0);
		bonsai_flush(&pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 1);	
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
int pnode_scan(struct pnode* first_node, pkey_t h_key, pval_t* val_arr) {
    struct pnode* pnode;
    int arr_size = 0;
    uint8_t* slot;
    int index;
  
    pnode = first_node;

    index = 1; slot = pnode->slot;
    while(index <= slot[0]) {
        if (key_cmp(pnode->e[slot[index]].k, h_key) >= 0)
            break;
        index++;
    }

	while(key_cmp(pnode->e[slot[1]].k, h_key) >= 0) {
        slot = pnode->slot;
        while(index <= slot[slot[0]]) {
            val_arr[arr_size] = pnode->e[slot[index]].k;
            arr_size++; index++;
        }
        pnode = list_next_entry(pnode, list);
        index = 1;
    }

    slot = pnode->slot;
    while(index <= slot[0]) {
        if (key_cmp(pnode->e[pnode->slot[index]].k, h_key) < 0)
            return arr_size;
        val_arr[arr_size] = pnode->e[slot[index]].k;
        arr_size++; index++;
    }
    
    return arr_size;
}

/*
 * pnode_lookup: lookup a key in the pnode
 * @pnode: persistent node
 * @key: target key
 */
pval_t* pnode_lookup(struct pnode* pnode, pkey_t key) {
	int bucket_id, i;

	kv_debug("pnode_lookup: pnode %016lx <%lu>\n", pnode, key);

	bucket_id = BUCKET_HASH(key);
	for (i = 0; i < NUM_ENT_PER_BUCKET; i ++) {
		if (key_cmp(pnode->e[bucket_id + i].k, key))
			return &pnode->e[bucket_id + i].v;
	}	

	return NULL;
}

/*
 * pnode_numa_move: move a bucket of entries to a remote NUMA node
 * @pnode: persistent node
 * @key: target key
 * @numa_node: remote NUMA node
 */
pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node) {
	struct pnode* remote_pnode = NULL;
	int bucket_id, i, offset;

	bucket_id = BUCKET_HASH(key);
	offset = bucket_id * NUM_ENT_PER_BUCKET;
	if (pnode->forward[numa_node][bucket_id] == 0) {
		remote_pnode = alloc_pnode(numa_node);
		memcpy(&remote_pnode[offset], &pnode->e[offset], 
            NUM_ENT_PER_BUCKET * sizeof(pentry_t));
		if (!cmpxchg2(&pnode->forward[numa_node][bucket_id], NULL, remote_pnode))
			free_pnode(pnode); /* fail */
	} else {
		memcpy(&remote_pnode->e[offset], &pnode->e[offset], 
            NUM_ENT_PER_BUCKET * sizeof(pentry_t));
	}

	bonsai_flush(&remote_pnode[offset], 
        NUM_ENT_PER_BUCKET * sizeof(pentry_t), 1);

	for (i = 0; i < NUM_ENT_PER_BUCKET; i++) {
		if (key_cmp(remote_pnode->e[i].k, key))
			return &remote_pnode->e[i].v;
	}
	
	return NULL;
}
