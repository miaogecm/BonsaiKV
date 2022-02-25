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
#include <assert.h>
#include <limits.h>
#include <libpmemobj.h>

#include "pnode.h"
#include "oplog.h"
#include "shim.h"
#include "bonsai.h"
#include "cpu.h"
#include "arch.h"
#include "bitmap.h"
#include "ordo.h"
#include "atomic128.h"
#include "per_node.h"

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, struct pnode);
POBJ_LAYOUT_END(BONSAI);

extern struct bonsai_info *bonsai;

#ifndef PNODE_FP
void print_pnode(struct pnode* pnode);

static uint64_t hash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

#define PNODE_BUCKET_HASH(x) (hash(x) % NUM_BUCKET)

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

	pnode->anchor_key = 0;

    pmemobj_persist(pop, pnode, sizeof(struct pnode));

    return pnode;
}

static void free_pnode(struct pnode* pnode) {
	PMEMoid toid = pmemobj_oid(pnode);
	int i;

	free(pnode->slot_lock);

	for (i = 0; i < NUM_BUCKET; i++)
		free(pnode->bucket_lock[i]);

	POBJ_FREE(&toid);
}

static void insert_pnode_list(struct pnode* pnode, pkey_t max_key) {
	struct pnode *pos, *prev = NULL;
	struct data_layer* layer = DATA(bonsai);
	struct list_head* head = &layer->pnode_list;

	write_lock(&layer->lock);
	list_for_each_entry(pos, head, list) {
		if (key_cmp(PNODE_ANCHOR_KEY(pos), max_key) > 0)
			break;
		else
			prev = pos;
	}

	if (!prev)
		list_add(&pnode->list, head);
	else
		__list_add(&pnode->list, &prev->list, &pos->list);
	write_unlock(&layer->lock);
}

static void insert_pnode_list_fast(struct pnode* pnode, struct pnode* next) {
	struct data_layer* layer = DATA(bonsai);
	struct pnode* prev;

#if 0
	prev = list_prev_entry(next, list);
	while(1) {
		if (cmpxchg2(&next->list.prev, &prev->list, &pnode->list)) {
			pnode->list.next = &next->list;
			break;
		}
	}
	while(1) {
		if (cmpxchg2(&prev->list.next, &next->list, &pnode->list)) {
			pnode->list.prev = &prev->list;
			break;
		}
	}
#else
	write_lock(&layer->lock);
	prev = list_prev_entry(next, list);
	__list_add(&pnode->list, &prev->list, &next->list);
	write_unlock(&layer->lock);
#endif
}

static void remove_pnode_list(struct pnode* pnode) {
	struct data_layer* layer = DATA(bonsai);
	
	write_lock(&layer->lock);
	list_del(&pnode->list);
	write_unlock(&layer->lock);
}

/*
 * find_unused_entry: return -1 if bucket is full.
 */
static int find_unused_entry(pkey_t key, uint64_t bitmap, int bucket_id) {
	uint64_t mask, pos;
	int offset;

	offset = NUM_ENT_PER_BUCKET * bucket_id;
	mask = (1UL << (offset + NUM_ENT_PER_BUCKET)) - (1UL << offset);
	bitmap = (bitmap & mask) | (~mask);
	
	if (bitmap == PNODE_BITMAP_FULL)
		return -1;

	pos = find_first_zero_bit(&bitmap, NUM_ENT_PER_PNODE);

    return pos;
}

/*
 * bucket_is_full: return 1 if bucket is full
 */
static int bucket_is_full(uint64_t bitmap, int bucket_id) {
	uint64_t mask;
	int offset;

	offset = NUM_ENT_PER_BUCKET * bucket_id;
	mask = (1UL << (offset + NUM_ENT_PER_BUCKET)) - (1UL << offset);
	bitmap = (bitmap & mask) | (~mask);

	if (bitmap == PNODE_BITMAP_FULL)
		return 1;

	return 0;
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

static void pnode_sort_slot(struct pnode* pnode, int pos_e, pkey_t key, optype_t op) {
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
 * pnode_find_lowbound: find first pnode whose maximum key is equal or greater than @key
 */
static struct pnode* pnode_find_lowbound(pkey_t key) {
	struct index_layer* layer = INDEX(bonsai);
	struct mptable* table;

	table = (struct mptable*)layer->lookup(layer->index_struct, key);
	assert(table);
	
	return this_node(&LOG(bonsai)->mptable_arena, table)->pnode;
}

/*
 * pnode_insert: insert a kv-pair into a pnode
 * @key: key
 * @val: value
 * @time_stamp: time stamp of the log
 * @numa_node: which numa node
 * return 0 if successful
 */
int pnode_insert(pkey_t key, pval_t value, unsigned long time_stamp, int numa_node) {
    int bucket_id, pos, i, n, d, mid_n;
	struct data_layer *layer = DATA(bonsai);
    struct pnode *new_pnode, *mid_pnode; 
	struct pnode *prev_node, *head_node = list_entry(&layer->pnode_list, struct pnode, list);
	uint64_t new_removed, mid_removed;
	int ret, cpu = __this->t_cpu;
	struct oplog* log;
	struct mptable* table;
	pval_t* addr;
	struct pnode* pnode;
	union atomic_u128 avg;
	pkey_t avg_key;

	bucket_id = PNODE_BUCKET_HASH(key);

retry:
	pnode = pnode_find_lowbound(key);

    table = this_node(&LOG(bonsai)->mptable_arena, pnode->table);
	
	assert(pnode);
	assert(key_cmp(key, PNODE_ANCHOR_KEY(pnode)) <= 0);

	bonsai_debug("thread[%d] <%lu %lu> find_lowbound: pnode %016lx max %lu\n", 
		get_tid(), key, value, pnode, PNODE_ANCHOR_KEY(pnode));
	
	write_lock(pnode->bucket_lock[bucket_id]);
	
	prev_node = list_prev_entry(pnode, list);

	if (prev_node != head_node && key_cmp(PNODE_ANCHOR_KEY(prev_node), key) >= 0) {
		write_unlock(pnode->bucket_lock[bucket_id]);
		goto retry;
	}

	addr = pnode_lookup(pnode, key);
	if (addr) {
		*addr = value;
		write_unlock(pnode->bucket_lock[bucket_id]);
		return 0;
	}

    pos = find_unused_entry(key, pnode->bitmap, bucket_id);
	
    if (pos != -1) {
		/* bucket is not full */
		pentry_t e = {key, value};
        pnode->e[pos] = e;

		addr = __mptable_lookup(table, key, cpu);
		if (!addr && ACCESS_ONCE(table->forward)) {
			addr = __mptable_lookup(table->forward, key, cpu);
		}

		//FIXME: need lock
		if (addr_in_log((unsigned long)addr)) {
			log = OPLOG(addr);
			ret = ordo_cmp_clock(log->o_stamp, time_stamp);
			if (ret == ORDO_LESS_THAN || ret == ORDO_UNCERTAIN) {
				mptable_update_addr(table, key, &pnode->e[pos].v);
			}
		} else {
			mptable_update_addr(table, key, &pnode->e[pos].v);
		}

		write_lock(pnode->slot_lock);
		pnode_sort_slot(pnode, pos, key, OP_INSERT);
		write_unlock(pnode->slot_lock);

		set_bit(pos, &pnode->bitmap);
		write_unlock(pnode->bucket_lock[bucket_id]);
		
		bonsai_flush((void*)&pnode->bitmap, sizeof(__le64), 0);
		bonsai_flush((void*)&pnode->e[pos], sizeof(pentry_t), 0);
		bonsai_flush((void*)&pnode->slot, NUM_ENT_PER_PNODE + 1, 1);

        return 0;
    }
	write_unlock(pnode->bucket_lock[bucket_id]);

	/* split the node */
    for (i = 0; i < NUM_BUCKET; i ++)
        write_lock(pnode->bucket_lock[i]);

	if (unlikely(!bucket_is_full(pnode->bitmap, bucket_id))) {
		/* re-check bucket because someone may remove an entry from the bucket */
		for (i = NUM_BUCKET - 1; i >= 0; i --) 
        	write_unlock(pnode->bucket_lock[i]);
		goto retry;
	}

    new_pnode = alloc_pnode(numa_node);
    memcpy(new_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENT_PER_PNODE);
    n = pnode->slot[0]; 
		d = n / 2;
    new_removed = 0;

    for (i = 1; i <= d; i++) {
    	new_removed |= (1ULL << pnode->slot[i]);
		new_pnode->slot[i] = pnode->slot[i];
    }
    new_pnode->slot[0] = d;
   	new_pnode->bitmap = new_removed;
	new_pnode->anchor_key = PNODE_MAX_KEY(new_pnode);

	mid_pnode = alloc_pnode(numa_node);
    memcpy(mid_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENT_PER_PNODE);
	mid_removed = 0;
	avg = (union atomic_u128) AtomicLoad128(&table->hs.avg);
	avg_key = (pkey_t) avg.lo;

	for (i = d + 1; i <= n; i++) {
		if (key_cmp(PNODE_SORTED_KEY(pnode, i), avg_key) > 0) {
			break;
		}
		mid_removed |= (1ULL << pnode->slot[i]);
		mid_pnode->slot[i - d] = pnode->slot[i];
	}
	mid_n = i - d - 1;
	mid_pnode->slot[0] = mid_n;
	mid_pnode->bitmap = mid_removed;
	mid_pnode->anchor_key = avg_key;

	if (key_cmp(avg_key, new_pnode->anchor_key) <= 0 
				|| key_cmp(avg_key, pnode->anchor_key) >= 0) {
		free_pnode(mid_pnode);
		mid_n = 0;
		mid_removed = 0;
		mid_pnode = NULL;
	}
	/* split the mapping table */
    mptable_split(table, new_pnode, mid_pnode, avg_key);

	insert_pnode_list_fast(new_pnode, pnode);
	if (mid_pnode) {
		insert_pnode_list_fast(mid_pnode, pnode);
	}

	for (i = 1; i <= n - d - mid_n; i ++) {
		pnode->slot[i] = pnode->slot[d + mid_n + i];
	}
	pnode->slot[0] = n - d - mid_n;
    pnode->bitmap &= ~new_removed;
	pnode->bitmap &= ~mid_removed;
	
    for (i = NUM_BUCKET - 1; i >= 0; i --) 
        write_unlock(pnode->bucket_lock[i]);

	bonsai_flush((void*)&pnode->bitmap, sizeof(__le64), 0);
	bonsai_flush((void*)&pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 0);
	bonsai_flush((void*)&new_pnode->bitmap, sizeof(__le64), 0);
	bonsai_flush((void*)&new_pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 1);

    goto retry;
}

/*
 * pnode_remove: remove an entry of a pnode
 * @key: key to be new_removed
 */
int pnode_remove(pkey_t key) {
	struct index_layer *i_layer = INDEX(bonsai);
  	int bucket_id, offset, i, node;
	uint64_t mask, bit;
	struct mptable* m;
	struct pnode* pnode;

	bonsai_debug("thread[%d] pnode_remove: <%lu>\n", __this->t_id, key);
	
	pnode = pnode_find_lowbound(key);
	assert(pnode);

    for_each_obj(&LOG(bonsai)->mptable_arena, node, m, pnode->table) {
		hs_remove(&m->hs, get_tid(), key);
    }

	bucket_id = PNODE_BUCKET_HASH(key);
    offset = NUM_ENT_PER_BUCKET * bucket_id;
	mask = (1ULL << (offset + NUM_ENT_PER_BUCKET)) - (1ULL << offset);

    write_lock(pnode->bucket_lock[bucket_id]);

	for (i = 0; i < NUM_ENT_PER_BUCKET; i ++) {
		bit = 1ULL << (offset + i);
		if ((pnode->bitmap & bit) && 
			key_cmp(pnode->e[offset + i].k, key) == 0)
			goto find;
	}
		
	write_unlock(pnode->bucket_lock[bucket_id]);

	return -ENOENT;

find:
    //mask |= (offset + i);
    //pnode->bitmap &= mask;
	/* FIXME: atomic instruction */
	clear_bit(bit, &pnode->bitmap);
	write_unlock(pnode->bucket_lock[bucket_id]);

    write_lock(pnode->slot_lock);
    pnode_sort_slot(pnode, i, key, OP_REMOVE);
    write_unlock(pnode->slot_lock);

	if (unlikely(!pnode->slot[0])) {
		i_layer->remove(i_layer->index_struct, PNODE_ANCHOR_KEY(pnode));
		remove_pnode_list(pnode);
        mptable_free(LOG(bonsai), pnode->table);
		free_pnode(pnode);
	} else {
		bonsai_flush((void*)&pnode->bitmap, sizeof(__le64), 0);
		bonsai_flush((void*)&pnode->slot, sizeof(NUM_ENT_PER_PNODE + 1), 1);	
	}

    return 0;
}

/*
 * scan_one_pnode: scan one pnode 
 * @pnode: scanned pnode
 * @n: index of result array
 * @high: end key
 * @result: result array
 */
int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr) {
	uint8_t* slot;
	pval_t max_key = 0;
	int index = 1;
	
	slot = pnode->slot;
	while(index <= slot[0]) {
		if (likely(key_cmp(PNODE_SORTED_KEY(pnode, index), high) <= 0 
				&& key_cmp(PNODE_SORTED_KEY(pnode, index), low) >= 0)) {
			result[n++] = PNODE_SORTED_VAL(pnode, index);
		}
		index++;
	}

	*curr = PNODE_ANCHOR_KEY(pnode);

	return n;
}

/*
 * pnode_lookup: lookup a key in the pnode
 * @pnode: persistent node
 * @key: target key
 */
pval_t* pnode_lookup(struct pnode* pnode, pkey_t key) {
	int bucket_id, i, offset;
	unsigned long mask;

	bonsai_debug("pnode_lookup: pnode %016lx <%lu>\n", pnode, key);

	bucket_id = PNODE_BUCKET_HASH(key);
	offset = bucket_id * NUM_ENT_PER_BUCKET;

	for (i = 0; i < NUM_ENT_PER_BUCKET; i ++) {
		mask = 1ULL << (offset + i);
		if ((pnode->bitmap & mask) && key_cmp(pnode->e[offset + i].k, key) == 0)
			return &pnode->e[offset + i].v;
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

	bucket_id = PNODE_BUCKET_HASH(key);
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

	bonsai_flush((void*)&remote_pnode[offset], 
        NUM_ENT_PER_BUCKET * sizeof(pentry_t), 1);

	for (i = 0; i < NUM_ENT_PER_BUCKET; i++) {
		if (key_cmp(remote_pnode->e[i].k, key))
			return &remote_pnode->e[i].v;
	}
	
	return NULL;
}

void sentinel_node_init() {
	struct index_layer *i_layer = INDEX(bonsai);
	struct mptable *table, *m;
	int numa_node = get_numa_node(get_cpu()), node;
	pkey_t key = ULONG_MAX;
	pval_t val = ULONG_MAX;
	struct pnode* pnode;
	int bucket_id, pos;
	pentry_t e = {key, val};
		
	/* LOG Layer: allocate a mapping table */
	table = mptable_alloc(&bonsai->l_layer);
	hs_insert(&table->hs, get_tid(), key, NULL);

	/* DATA Layer: allocate a persistent node */
	pnode = alloc_pnode(numa_node);
	pnode->anchor_key = key;
	insert_pnode_list(pnode, key);
    for_each_obj(&LOG(bonsai)->mptable_arena, node, m, table) {
        m->pnode = pnode;
        m->lowerbound = ULONG_MAX;
    }
	pnode->table = table;

	bucket_id = PNODE_BUCKET_HASH(key);
    write_lock(pnode->bucket_lock[bucket_id]);
    pos = find_unused_entry(key, pnode->bitmap, bucket_id);
    pnode->e[pos] = e;

    mptable_update_addr(table, key, &pnode->e[pos].v);

    write_lock(pnode->slot_lock);
    pnode_sort_slot(pnode, pos, key, OP_INSERT);
    write_unlock(pnode->slot_lock);

	set_bit(pos, &pnode->bitmap);
    write_unlock(pnode->bucket_lock[bucket_id]);
		
	bonsai_flush((void*)&pnode->bitmap, sizeof(__le64), 0);
	bonsai_flush((void*)&pnode->e[pos], sizeof(pentry_t), 0);
	bonsai_flush((void*)&pnode->slot, NUM_ENT_PER_PNODE + 1, 1);

	/* INDEX Layer: insert into index layer */
	i_layer->insert(i_layer->index_struct, key, (void*)table);

	bonsai_debug("sentinel_node_init\n");
}

/*
 * check_pnode: validate this pnode
 */
void check_pnode(pkey_t key, struct pnode* pnode) {
	unsigned long bitmap = 0;
	pkey_t prev = 0, curr = ULONG_MAX;
	int i, die = 0;

	for (i = 1; i <= pnode->slot[0]; i ++) {
		bitmap |= (1<<pnode->slot[i]);
	}

#if 0
	/* check bitmap */
	if (bitmap != pnode->bitmap) {
		bonsai_print("pnode bitmap mismatch: %016lx %016lx\n", bitmap, pnode->bitmap);
		goto die;
	}
#endif

	/* check pnode entries */
	for (i = 1; i <= pnode->slot[0]; i ++) {
			curr = PNODE_SORTED_KEY(pnode, i);
			if (curr < prev) {
				bonsai_print("pnode bad key order key[%d]:%lu key[%d]:%lu [%lu]\n", 
							i - 1, prev, i, curr, key);
				goto die;	
			}
			prev = curr;
	}
	return;

die:
	print_pnode(pnode);
	assert(0);
}

void print_pnode(struct pnode* pnode) {
	int i;
	
	bonsai_print("pnode == bitmap: %016lx slot[0]: %d [%lu %lu]\n", 
			pnode->bitmap, pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode));
	
	for (i = 0; i <= pnode->slot[0]; i ++)
		bonsai_print("slot[%d]: %d; ", i, pnode->slot[i]);
	bonsai_print("\n");

	for (i = 1; i <= pnode->slot[0]; i ++)
		bonsai_print("key[%d]: %lu; ", i, PNODE_SORTED_KEY(pnode, i));
	bonsai_print("\n");
	
	for (i = 0; i < NUM_ENT_PER_PNODE; i ++)
		bonsai_print("[%d]: <%lu, %lu> ", i, pnode->e[i].k, pnode->e[i].v);
	bonsai_print("\n");
}

void print_pnode_summary(struct pnode* pnode) {
	bonsai_print("pnode == bitmap: %016lx slot[0]: %d [%lu %lu]\n", pnode->bitmap, pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode));
}

void dump_pnode_list_summary() {
	struct data_layer *layer = DATA(bonsai);
	struct pnode* pnode;
	int j = 0, pnode_sum = 0, key_sum = 0;

	bonsai_print("====================================================================================\n");

	read_lock(&layer->lock);
	list_for_each_entry(pnode, &layer->pnode_list, list) {
		bonsai_debug("[pnode[%d]{%016lx} == bitmap: %016lx slot[0]: %d max: %lu anchor: %lu]\n", j++, pnode, pnode->bitmap, pnode->slot[0], PNODE_MAX_KEY(pnode), PNODE_ANCHOR_KEY(pnode));
		key_sum += pnode->slot[0];
		pnode_sum ++;
	}
	read_unlock(&layer->lock);

	bonsai_print("total pnode [%d]\n", pnode_sum);
	bonsai_print("data layer total entries [%d]\n", key_sum);
}

void dump_pnode_list() {
	struct data_layer *layer = DATA(bonsai);
	struct pnode* pnode;
	int i, j = 0, pnode_sum = 0, key_sum = 0;
	pkey_t prev = 0, curr = ULONG_MAX;

	bonsai_print("====================================DUMP PNODE LIST================================================\n");

	read_lock(&layer->lock);
	list_for_each_entry(pnode, &layer->pnode_list, list) {
		bonsai_print("pnode[%d] == bitmap: %016lx slot[0]: %d [%lu %lu]\n", j++, pnode->bitmap, pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode));
		key_sum += pnode->slot[0];
		pnode_sum ++;

		for (i = 1; i <= pnode->slot[0]; i ++) 
			bonsai_print("slot[%d]: %d; ", i, pnode->slot[i]);
		bonsai_print("\n");

		for (i = 1; i <= pnode->slot[0]; i ++) {
			curr = PNODE_SORTED_KEY(pnode, i);
			bonsai_print("key[%d]: %lu; ", i, PNODE_SORTED_KEY(pnode, i));
			if (curr < prev) assert(0);
			prev = curr;
		}
		bonsai_print("\n");
	
		for (i = 0; i < NUM_ENT_PER_PNODE; i ++)
			bonsai_print("key[%d]: %lu; ", i, pnode->e[i].k);
		bonsai_print("\n");
	}
	read_unlock(&layer->lock);

	bonsai_print("total pnode [%d]\n", pnode_sum);
	bonsai_print("pnode list total key [%d]\n", key_sum);
}

/*
 * data_layer_search_key: search @key in data layer, you must stop the world first.
 */
struct pnode* data_layer_search_key(pkey_t key) {
	struct data_layer *layer = DATA(bonsai);
	struct pnode* pnode = NULL;
	int i, total = 0;
	
	list_for_each_entry(pnode, &layer->pnode_list, list) {
		for (i = 1; i < pnode->slot[0]; i ++) {
			if (!key_cmp(PNODE_SORTED_KEY(pnode, i), key)) {
				bonsai_print("data layer search key[%lu]: pnode %016lx key index %d, val address %016lx\n", key, pnode, pnode->slot[i], &pnode->e[pnode->slot[i]].v);
				print_pnode(pnode);
				return pnode;
			}
		}
	}

	bonsai_print("data layer find no such key[%lu]\n", key);

	return NULL;
}
#else

static uint8_t hash8(pkey_t key) {
	return key % 256;
}

pval_t* pnode_lookup(struct pnode* pnode, pkey_t key) {
	int h, i, j;

	h = hash8(key);
	read_lock(&PNODE_LOCK(pnode));

	for (i = 0; i < NUM_ENTRY_PER_PNODE; i++) {
		if (PNODE_BITMAP(pnode) & (1ULL << i)) {
			if (PNODE_FGPRT(pnode, i) == h) {
				if (key_cmp(PNODE_KEY(pnode, i), key) == 0) {
					read_unlock(&PNODE_LOCK(pnode));
					return &PNODE_VAL(pnode, i);
				}
			}
		}
	}

	read_unlock(&PNODE_LOCK(pnode));
	return NULL;
}

/*
 * pnode_find_lowbound: find first pnode whose maximum key is equal or greater than @key
 */
static struct pnode* pnode_find_lowbound(pkey_t key) {
	struct index_layer* layer = INDEX(bonsai);
	struct mptable* table;

	table = (struct mptable*)layer->lookup(layer->index_struct, key);
	assert(table);
	
	return this_node(&LOG(bonsai)->mptable_arena, table)->pnode;
}

static int find_unused_entry(uint64_t bitmap) {
	int pos;

	if (bitmap == PNODE_BITMAP_FULL)
		return -1;

	pos = find_first_zero_bit(&bitmap, NUM_ENTRY_PER_PNODE);

    return pos;
}

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

	PNODE_BITMAP(pnode) = cpu_to_le64(0);

    rwlock_init(&PNODE_LOCK(pnode));

	INIT_LIST_HEAD(&pnode->list);

	memset(pnode->slot, 0, NUM_ENTRY_PER_PNODE + 1);

	pnode->table = NULL;

	memset(pnode->forward, 0, NUM_SOCKET * NUM_BUCKET * sizeof(__le64));

	pnode->anchor_key = 0;

    pmemobj_persist(pop, pnode, sizeof(struct pnode));

    return pnode;
}

static int lower_bound(struct pnode* pnode, pkey_t key) {
    uint8_t* slot;
    int l, r, res;

    if (pnode == NULL) 
        return 0;
    
    slot = pnode->slot;
    l = 1;
    r = slot[0] + 1;
	res = r;

    while(l < r) {
        int mid;

        mid = l + (r - l) / 2;
        if (key_cmp(PNODE_SORTED_KEY(pnode, mid), key) >= 0) {
            r = mid;
			res = mid;
		}
        else 
            l = mid + 1;
    }

    return res;
}

static void pnode_sort_slot(struct pnode* pnode, int pos_e, pkey_t key, optype_t op) {
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

static void insert_pnode_list_fast(struct pnode* pnode, struct pnode* next) {
	struct data_layer* layer = DATA(bonsai);
	struct pnode* prev;

#if 0
	prev = list_prev_entry(next, list);
	while(1) {
		if (cmpxchg2(&next->list.prev, &prev->list, &pnode->list)) {
			pnode->list.next = &next->list;
			break;
		}
	}
	while(1) {
		if (cmpxchg2(&prev->list.next, &next->list, &pnode->list)) {
			pnode->list.prev = &prev->list;
			break;
		}
	}
#else
	write_lock(&layer->lock);
	prev = list_prev_entry(next, list);
	__list_add(&pnode->list, &prev->list, &next->list);
	write_unlock(&layer->lock);
#endif
}

int pnode_insert(pkey_t key, pval_t val, unsigned long time_stamp, int numa_node) {
    int bucket_id, pos, i, n, d, mid_n;
	uint64_t table_cnt;
	struct data_layer *layer = DATA(bonsai);
    struct pnode *new_pnode, *mid_pnode; 
	struct pnode *prev_node, *head_node = list_entry(&layer->pnode_list, struct pnode, list);
	uint64_t new_removed, mid_removed;
	int ret, cpu = __this->t_cpu;
	struct oplog* log;
	struct mptable* table;
	pval_t* addr;
	struct pnode* pnode;
	union atomic_u128 avg;
	pkey_t avg_key;

retry:
	pnode = pnode_find_lowbound(key);

	table = this_node(&LOG(bonsai)->mptable_arena, pnode->table);
	
	assert(pnode);
	assert(key_cmp(key, PNODE_ANCHOR_KEY(pnode)) <= 0);

	write_lock(&PNODE_LOCK(pnode));

	prev_node = list_prev_entry(pnode, list);
	if (prev_node != head_node && key_cmp(PNODE_ANCHOR_KEY(prev_node), key) >= 0) {
		write_unlock(&PNODE_LOCK(pnode));
		goto retry;
	}

	pos = find_unused_entry(PNODE_BITMAP(pnode));

	if (pos != -1) {
		PNODE_KEY(pnode, pos) = key;
		PNODE_VAL(pnode, pos) = val;
		bonsai_flush(&(PNODE_KEY(pnode, pos)), sizeof(pentry_t), 1);

		addr = __mptable_lookup(table, key, cpu);
		if (!addr && ACCESS_ONCE(table->forward)) {
			addr = __mptable_lookup(table->forward, key, cpu);
		}

		if (addr_in_log((unsigned long)addr)) {
			log = OPLOG(addr);
			ret = ordo_cmp_clock(log->o_stamp, time_stamp);
			if (ret == ORDO_LESS_THAN || ret == ORDO_UNCERTAIN) {
				mptable_update_addr(table, key, &PNODE_VAL(pnode, pos));
			}
		} else {
			mptable_update_addr(table, key, &PNODE_VAL(pnode, pos));
		}
		
		pnode_sort_slot(pnode, pos, key, OP_INSERT);
		bonsai_flush(&pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);

		PNODE_FGPRT(pnode, pos) = hash8(key);
		set_bit(pos, &PNODE_BITMAP(pnode));
		bonsai_flush(&PNODE_BITMAP(pnode), sizeof(__le64), 1);

		write_unlock(&PNODE_LOCK(pnode));

		return 0;
	}

	new_pnode = alloc_pnode(numa_node);
	memcpy(new_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENTRY_PER_PNODE);
	memcpy(&PNODE_FGPRT(new_pnode, 0), &PNODE_FGPRT(pnode, 0), NUM_ENTRY_PER_PNODE);

	n = pnode->slot[0];
	d = n / 2;
	new_removed = 0;

	for (i = 1; i <= d; i++) {
		new_removed |= (1ULL << pnode->slot[i]);
		new_pnode->slot[i] = pnode->slot[i];
	}
	new_pnode->slot[0] = d;
	PNODE_BITMAP(new_pnode) = new_removed;
	new_pnode->anchor_key = PNODE_MAX_KEY(new_pnode);
	
	mid_removed = 0;
	avg = (union atomic_u128) AtomicLoad128(&table->hs.avg);
	avg_key = (pkey_t) avg.lo;
	table_cnt = avg.hi;

	if (table_cnt < SMALL_HASHSET_MAXN
		|| key_cmp(avg_key, new_pnode->anchor_key) <= 0 
		|| key_cmp(avg_key, pnode->anchor_key) >= 0) {
		mid_n = 0;
		mid_removed = 0;
		mid_pnode = NULL;
	} else {
		mid_pnode = alloc_pnode(numa_node);
		memcpy(mid_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENTRY_PER_PNODE);
		memcpy(&PNODE_FGPRT(mid_pnode, 0), &PNODE_FGPRT(pnode, 0), NUM_ENTRY_PER_PNODE);
		
		for (i = d + 1; i <= n; i++) {
			if (key_cmp(PNODE_SORTED_KEY(pnode, i), avg_key) > 0) {
				break;
			}
			mid_removed |= (1ULL << pnode->slot[i]);
			mid_pnode->slot[i - d] = pnode->slot[i];
		}
		mid_n = i - d - 1;
		mid_pnode->slot[0] = mid_n;
		PNODE_BITMAP(mid_pnode) = mid_removed;
		mid_pnode->anchor_key = avg_key;
	}

	mptable_split(table, new_pnode, mid_pnode, avg_key);
	
	insert_pnode_list_fast(new_pnode, pnode);
	if (mid_pnode) {
		insert_pnode_list_fast(mid_pnode, pnode);
	}

	for (i = 1; i <= n - d - mid_n; i ++) {
		pnode->slot[i] = pnode->slot[d + mid_n + i];
	}
	pnode->slot[0] = n - d - mid_n;
    PNODE_BITMAP(pnode) &= ~new_removed;
	PNODE_BITMAP(pnode) &= ~mid_removed;

	bonsai_flush((void*)&new_pnode->e, sizeof(pentry_t) * NUM_ENTRY_PER_PNODE, 1);
	bonsai_flush((void*)&new_pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
	bonsai_flush((void*)&PNODE_BITMAP(new_pnode), sizeof(__le64), 1);
	if (mid_pnode) {
		bonsai_flush((void*)&mid_pnode->e, sizeof(pentry_t) * NUM_ENTRY_PER_PNODE, 1);
		bonsai_flush((void*)&mid_pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
		bonsai_flush((void*)&PNODE_BITMAP(mid_pnode), sizeof(__le64), 1);
	}
	bonsai_flush((void*)&pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
	bonsai_flush((void*)&PNODE_BITMAP(pnode), sizeof(__le64), 1);
	
	write_unlock(&PNODE_LOCK(pnode));

	goto retry;
}

static void free_pnode(struct pnode* pnode) {
#if 0
	PMEMoid toid = pmemobj_oid(pnode);

	POBJ_FREE(&toid);
#endif
}

static void remove_pnode_list(struct pnode* pnode) {
	struct data_layer* layer = DATA(bonsai);
	
	write_lock(&layer->lock);
	list_del(&pnode->list);
	write_unlock(&layer->lock);
}

static void pnode_insert_one(struct pnode* pnode, pkey_t key, pval_t val, struct mptable* table) {
	pval_t* addr;
	int pos, ret;
	
	pos = find_unused_entry(PNODE_BITMAP(pnode));

	PNODE_KEY(pnode, pos) = key;
	PNODE_VAL(pnode, pos) = val;
	bonsai_flush(&(PNODE_KEY(pnode, pos)), sizeof(pentry_t), 1);

	addr = __mptable_lookup(table, key, 0);
	if (!addr && ACCESS_ONCE(table->forward)) {
		addr = __mptable_lookup(table->forward, key, 0);
	}

	if (addr_in_log((unsigned long)addr)) {
		mptable_update_addr(table, key, &PNODE_VAL(pnode, pos));
	} else {
		mptable_update_addr(table, key, &PNODE_VAL(pnode, pos));
	}

	pnode_sort_slot(pnode, pos, key, OP_INSERT);
	
	PNODE_FGPRT(pnode, pos) = hash8(key);
	set_bit(pos, &PNODE_BITMAP(pnode));
}

static int pnode_merge(struct pnode* pnode, struct pnode* near) {
	int i;
	struct mptable* table;

	table = this_node(&LOG(bonsai)->mptable_arena, pnode->table);

	for (i = 0; i < pnode->slot[0]; i++) {
		pnode_insert_one(near, PNODE_KEY(pnode, i), PNODE_VAL(pnode, i), table);
	}
	
	bonsai_flush((void*)&near->slot, NUM_ENTRY_PER_PNODE + 1, 0);	
	bonsai_flush((void*)&PNODE_BITMAP(near), sizeof(__le64), 1);

	list_del(pnode);
}

int pnode_remove(pkey_t key) {
	struct index_layer *i_layer = INDEX(bonsai);
	struct data_layer* d_layer = DATA(bonsai);
	struct mptable* m;
	struct pnode *pnode, *near;
	int h, pos, i, node;
	
	pnode = pnode_find_lowbound(key);
	assert(pnode);

	for_each_obj(&LOG(bonsai)->mptable_arena, node, m, pnode->table) {
		hs_remove(&m->hs, get_tid(), key);
    }

	write_lock(&PNODE_LOCK(pnode));

	h = hash8(key);
	pos = -1;

	for (i = 0; i < NUM_ENTRY_PER_PNODE; i++) {
		if (PNODE_FGPRT(pnode, i) == h) {
			if (key_cmp(PNODE_KEY(pnode, i), key) == 0) {
				pos = i;
				break;
			}
		}
	}

	if (unlikely(pos == -1)) {
		assert(0);
		write_unlock(&PNODE_LOCK(pnode));
		return -ENOENT;
	}

	pnode_sort_slot(pnode, pos, key, OP_REMOVE);
	clear_bit((1ULL << pos), &PNODE_BITMAP(pnode));

	if (unlikely(!pnode->slot[0])) {
		i_layer->remove(i_layer->index_struct, PNODE_ANCHOR_KEY(pnode));
		remove_pnode_list(pnode);
        mptable_free(LOG(bonsai), pnode->table);
		free_pnode(pnode);
	} else if (pnode->slot[0] > NUM_ENTRY_PER_PNODE / 2) {
		bonsai_flush((void*)&pnode->slot, sizeof(NUM_ENTRY_PER_PNODE + 1), 0);	
		bonsai_flush((void*)&PNODE_BITMAP(pnode), sizeof(__le64), 1);
		write_unlock(&PNODE_LOCK(pnode));
	} else {
		// merge
		near = list_next_entry(pnode, list);
		if (near != list_entry(&d_layer->pnode_list, struct pnode, list)) {
			write_lock(&PNODE_LOCK(near));
			if (near->slot[0] + pnode->slot[0] <= NUM_ENTRY_PER_PNODE) {
				pnode_merge(pnode, near);
			}
			write_unlock(&PNODE_LOCK(near));
		}
		write_unlock(&PNODE_LOCK(pnode));
	}

	return 0;
}

static void insert_pnode_list(struct pnode* pnode, pkey_t max_key) {
	struct pnode *pos, *prev = NULL;
	struct data_layer* layer = DATA(bonsai);
	struct list_head* head = &layer->pnode_list;

	write_lock(&layer->lock);
	list_for_each_entry(pos, head, list) {
		if (key_cmp(PNODE_ANCHOR_KEY(pos), max_key) > 0)
			break;
		else
			prev = pos;
	}

	if (!prev)
		list_add(&pnode->list, head);
	else
		__list_add(&pnode->list, &prev->list, &pos->list);
	write_unlock(&layer->lock);
}

void sentinel_node_init() {
	struct index_layer *i_layer = INDEX(bonsai);
	struct mptable *table, *m;
	int numa_node = get_numa_node(get_cpu()), node;
	pkey_t key = ULONG_MAX;
	pval_t val = ULONG_MAX;
	struct pnode* pnode;
	int bucket_id, pos;
	pentry_t e = {key, val};
		
	/* LOG Layer: allocate a mapping table */
	table = mptable_alloc(&bonsai->l_layer);
	hs_insert(&table->hs, get_tid(), key, NULL);

	/* DATA Layer: allocate a persistent node */
	pnode = alloc_pnode(numa_node);
	pnode->anchor_key = key;
	insert_pnode_list(pnode, key);
    for_each_obj(&LOG(bonsai)->mptable_arena, node, m, table) {
        m->pnode = pnode;
        m->lowerbound = ULONG_MAX;
    }
	pnode->table = table;

	write_lock(&PNODE_LOCK(pnode));

	pos = find_unused_entry(PNODE_BITMAP(pnode));
	pnode->e[pos] = e;
	PNODE_FGPRT(pnode, pos) = hash8(key);
	pnode_sort_slot(pnode, pos, key, OP_INSERT);
	set_bit(pos, &PNODE_BITMAP(pnode));

	bonsai_flush((void*)&pnode->e[pos], sizeof(pentry_t), 1);
	bonsai_flush((void*)&pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
	bonsai_flush((void*)&PNODE_BITMAP(pnode), sizeof(__le64), 1);
	
	write_unlock(&PNODE_LOCK(pnode));

	mptable_update_addr(table, key, &pnode->e[pos].v);

	/* INDEX Layer: insert into index layer */
	i_layer->insert(i_layer->index_struct, key, (void*)table);

	bonsai_debug("sentinel_node_init\n");
}

/*
 * pnode_numa_move: move a bucket of entries to a remote NUMA node
 * @pnode: persistent node
 * @key: target key
 * @numa_node: remote NUMA node
 */
pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node, void* addr) {
	struct pnode* remote_pnode = NULL;
	int bucket_id, i, offset;

	bucket_id = ((uint64_t)addr - (uint64_t)pnode->e) / CACHELINE_SIZE;
	offset = bucket_id * NUM_ENT_PER_BUCKET;

	if (pnode->forward[numa_node][bucket_id] == 0) {
		remote_pnode = alloc_pnode(numa_node);
		memcpy(&remote_pnode->e[offset], &pnode->e[offset], 
            NUM_ENT_PER_BUCKET * sizeof(pentry_t));
		if (!cmpxchg2(&pnode->forward[numa_node][bucket_id], NULL, remote_pnode))
			free_pnode(pnode); /* fail */
	} else {
		memcpy(&remote_pnode->e[offset], &pnode->e[offset], 
            NUM_ENT_PER_BUCKET * sizeof(pentry_t));
	}

	bonsai_flush((void*)&remote_pnode[offset], 
        NUM_ENT_PER_BUCKET * sizeof(pentry_t), 1);

	for (i = 0; i < NUM_ENT_PER_BUCKET; i++) {
		if (key_cmp(remote_pnode->e[i].k, key))
			return &remote_pnode->e[i].v;
	}
	
	return NULL;
}

int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr) {
	uint8_t* slot;
	pval_t max_key = 0;
	int index = 1;
	
	slot = pnode->slot;
	while(index <= slot[0]) {
		if (likely(key_cmp(PNODE_SORTED_KEY(pnode, index), high) <= 0 
				&& key_cmp(PNODE_SORTED_KEY(pnode, index), low) >= 0)) {
			result[n++] = PNODE_SORTED_VAL(pnode, index);
		}
		index++;
	}

	*curr = PNODE_ANCHOR_KEY(pnode);

	return n;
}

/*
 * check_pnode: validate this pnode
 */
void check_pnode(pkey_t key, struct pnode* pnode) {
	unsigned long bitmap = 0;
	pkey_t prev = 0, curr = ULONG_MAX;
	int i, die = 0;

	for (i = 1; i <= pnode->slot[0]; i ++) {
		bitmap |= (1ULL << pnode->slot[i]);
	}

#if 0
	/* check bitmap */
	if (bitmap != pnode->bitmap) {
		bonsai_print("pnode bitmap mismatch: %016lx %016lx\n", bitmap, pnode->bitmap);
		goto die;
	}
#endif

	/* check pnode entries */
	for (i = 1; i <= pnode->slot[0]; i ++) {
			curr = PNODE_SORTED_KEY(pnode, i);
			if (curr < prev) {
				bonsai_print("pnode bad key order key[%d]:%lu key[%d]:%lu [%lu]\n", 
							i - 1, prev, i, curr, key);
				goto die;	
			}
			prev = curr;
	}
	return;

die:
	print_pnode(pnode);
	assert(0);
}

void print_pnode(struct pnode* pnode) {
	int i;
	
	bonsai_print("pnode == bitmap: %016lx slot[0]: %d [%lu %lu] anchor{%lu}:\n", 
			PNODE_BITMAP(pnode), pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode), PNODE_ANCHOR_KEY(pnode));
	
	for (i = 0; i <= pnode->slot[0]; i ++)
		bonsai_print("slot[%d]: %d; ", i, pnode->slot[i]);
	bonsai_print("\n");

	for (i = 1; i <= pnode->slot[0]; i ++)
		bonsai_print("key[%d]: %lu; ", i, PNODE_SORTED_KEY(pnode, i));
	bonsai_print("\n");
	
	for (i = 0; i < NUM_ENTRY_PER_PNODE; i ++)
		bonsai_print("fp[%d]: %d; ", i, PNODE_FGPRT(pnode, i));
	bonsai_print("\n");
	for (i = 0; i < NUM_ENTRY_PER_PNODE; i ++)
		bonsai_print("[%d]: <%lu, %lu> ", i, pnode->e[i].k, pnode->e[i].v);
	bonsai_print("\n");
}

void print_pnode_summary(struct pnode* pnode) {
	bonsai_print("pnode == bitmap: %016lx slot[0]: %d [%lu %lu] anchor{%lu}\n", PNODE_BITMAP(pnode), pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode), PNODE_ANCHOR_KEY(pnode));
}

void dump_pnode_list_summary() {
	struct data_layer *layer = DATA(bonsai);
	struct pnode* pnode;
	int j = 0, pnode_sum = 0, key_sum = 0;

	bonsai_print("====================================================================================\n");

	read_lock(&layer->lock);
	list_for_each_entry(pnode, &layer->pnode_list, list) {
		bonsai_debug("[pnode[%d]{%016lx} == bitmap: %016lx slot[0]: %d max: %lu anchor: %lu]\n", j++, pnode, PNODE_BITMAP(pnode), pnode->slot[0], PNODE_MAX_KEY(pnode), PNODE_ANCHOR_KEY(pnode));
		key_sum += pnode->slot[0];
		pnode_sum ++;
	}
	read_unlock(&layer->lock);

	bonsai_print("total pnode [%d]\n", pnode_sum);
	bonsai_print("data layer total entries [%d]\n", key_sum);
}

void dump_pnode_list() {
	struct data_layer *layer = DATA(bonsai);
	struct pnode* pnode;
	int i, j = 0, pnode_sum = 0, key_sum = 0;
	pkey_t prev = 0, curr = ULONG_MAX;

	bonsai_print("====================================DUMP PNODE LIST================================================\n");

	read_lock(&layer->lock);
	list_for_each_entry(pnode, &layer->pnode_list, list) {
		bonsai_print("pnode[%d] == bitmap: %016lx slot[0]: %d [%lu %lu]\n", j++, PNODE_BITMAP(pnode), pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode));
		key_sum += pnode->slot[0];
		pnode_sum ++;

		for (i = 1; i <= pnode->slot[0]; i ++) 
			bonsai_print("slot[%d]: %d; ", i, pnode->slot[i]);
		bonsai_print("\n");

		for (i = 1; i <= pnode->slot[0]; i ++) {
			curr = PNODE_SORTED_KEY(pnode, i);
			bonsai_print("key[%d]: %lu; ", i, PNODE_SORTED_KEY(pnode, i));
			if (curr < prev) assert(0);
			prev = curr;
		}
		bonsai_print("\n");
	
		for (i = 0; i < NUM_ENTRY_PER_PNODE; i ++)
			bonsai_print("key[%d]: %lu; ", i, pnode->e[i].k);
		bonsai_print("\n");
	}
	read_unlock(&layer->lock);

	bonsai_print("total pnode [%d]\n", pnode_sum);
	bonsai_print("pnode list total key [%d]\n", key_sum);
}

/*
 * data_layer_search_key: search @key in data layer, you must stop the world first.
 */
struct pnode* data_layer_search_key(pkey_t key) {
	struct data_layer *layer = DATA(bonsai);
	struct pnode* pnode = NULL;
	int i, total = 0;
	
	list_for_each_entry(pnode, &layer->pnode_list, list) {
		for (i = 1; i < pnode->slot[0]; i ++) {
			if (!key_cmp(PNODE_SORTED_KEY(pnode, i), key)) {
				bonsai_print("data layer search key[%lu]: pnode %016lx key index %d, val address %016lx\n", key, pnode, pnode->slot[i], &pnode->e[pnode->slot[i]].v);
				print_pnode(pnode);
				return pnode;
			}
		}
	}

	bonsai_print("data layer find no such key[%lu]\n", key);

	return NULL;
}
#endif