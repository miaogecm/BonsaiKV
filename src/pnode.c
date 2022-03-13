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

static uint8_t hash8(pkey_t key) {
	return key % 256;
}

/*
 * pnode_find_lowbound: find last pnode whose minimum key is equal or less than @key
 */
static struct pnode *pnode_find_lowbound(pkey_t key) {
	struct index_layer* layer = INDEX(bonsai);
    int node = get_numa_node(__this->t_cpu);
    struct pnode *pnode;
    const void *i_key;
    uint64_t aux;
    size_t len;

    /* Note that @key should be in local NUMA node. */
    i_key = resolve_key(key, &aux, &len);

	pnode = (struct pnode*)layer->lookup(layer->pnode_index_struct[node], i_key, len);

    return pnode;
}

static int find_unused_entry(uint64_t bitmap) {
	int pos;

	if (bitmap == PNODE_BITMAP_FULL)
		return -1;

	pos = find_first_zero_bit(&bitmap, NUM_ENTRY_PER_PNODE);

    return pos;
}

static struct pnode *alloc_pnode(int node) {
	struct data_layer *layer = DATA(bonsai);
    struct pnode *pnode, *my;
    TOID(struct pnode) toid;

    POBJ_ALLOC(layer->region[0].pop, &toid, struct pnode, sizeof(struct pnode) + CACHELINE_SIZE, NULL, NULL);
    pnode = PTR_ALIGN(pmemobj_direct(toid.oid), CACHELINE_SIZE);

    my = node_ptr(pnode, node, 0);
    PNODE_ANCHOR_KEY(my) = 0;
	PNODE_BITMAP(my) = cpu_to_le64(0);
    PNODE_LOCK(my) = malloc(sizeof(rwlock_t));
    rwlock_init(PNODE_LOCK(my));
    my->next = NULL;
	memset(my->slot, 0, NUM_ENTRY_PER_PNODE + 1);

    pmemobj_persist(layer->region[node].pop, my, sizeof(struct pnode));

    return my;
}

static int lower_bound(struct pnode *pnode, pkey_t key) {
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

        if (pkey_compare(PNODE_SORTED_KEY(pnode, mid), key) >= 0) {
            r = mid;
			res = mid;
		}
        else 
            l = mid + 1;
    }

    return res;
}

static void pnode_sort_slot(struct pnode *pnode, int pos_e, pkey_t key, optype_t op) {
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

int pnode_insert(pkey_t key, pval_t val, pval_t *old) {
    struct index_layer *i_layer = INDEX(bonsai);
	struct pnode *pnode, *next_pnode, *new_pnode;
    int node = get_numa_node(__this->t_cpu);
    uint64_t new_removed;
    const void *i_key;
    int pos, i, n, d;
    uint64_t aux;
    size_t len;

	pnode = pnode_find_lowbound(key);
	
	assert(pnode);
 	assert(pkey_compare(key, PNODE_ANCHOR_KEY(pnode)) >= 0);

retry:
	write_lock(PNODE_LOCK(pnode));

    if (pnode->next && pkey_compare(pnode->next->anchor_key, key) <= 0) {
        /* pnode split between find_lowerbound and write_lock */
        next_pnode = pnode->next;
        write_unlock(PNODE_LOCK(pnode));
        pnode = next_pnode;
        goto retry;
    }

	pos = find_unused_entry(PNODE_BITMAP(pnode));

	if (pos != -1) {
        PNODE_KEY(pnode, pos) = key;
        PNODE_VAL(pnode, pos) = val;
		bonsai_flush(&(PNODE_KEY(pnode, pos)), sizeof(pentry_t), 1);

        shim_update(key, old, &PNODE_VAL(pnode, pos));
		
		pnode_sort_slot(pnode, pos, key, OP_INSERT);
		bonsai_flush(&pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);

        PNODE_FGPRT(pnode, pos) = hash8(key);
		__set_bit(pos, &PNODE_BITMAP(pnode));
		bonsai_flush(&PNODE_BITMAP(pnode), sizeof(__le64), 1);

		write_unlock(PNODE_LOCK(pnode));

		return 0;
	}

	new_pnode = alloc_pnode(node);
	memcpy(new_pnode->e, pnode->e, sizeof(pentry_t) * NUM_ENTRY_PER_PNODE);
	memcpy(&PNODE_FGPRT(new_pnode, 0), &PNODE_FGPRT(pnode, 0), NUM_ENTRY_PER_PNODE);

	n = pnode->slot[0];
	d = n / 2;
	new_removed = 0;

	for (i = 1; i <= d; i++) {
		new_removed |= (1ULL << pnode->slot[n - d + i]);
		new_pnode->slot[i] = pnode->slot[n - d + i];
	}
	new_pnode->slot[0] = d;
    PNODE_BITMAP(new_pnode) = new_removed;

    new_pnode->anchor_key = PNODE_MIN_KEY(new_pnode);
    new_pnode->next = pnode->next;
    pnode->next = new_pnode;

    assert(new_pnode->anchor_key > pnode->anchor_key);

	pnode->slot[0] = n - d;
    PNODE_BITMAP(pnode) &= ~new_removed;

	bonsai_flush((void*)&new_pnode->e, sizeof(pentry_t) * NUM_ENTRY_PER_PNODE, 1);
	bonsai_flush((void*)&new_pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
	bonsai_flush((void*)&PNODE_BITMAP(new_pnode), sizeof(__le64), 1);
	bonsai_flush((void*)&pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
	bonsai_flush((void*)&PNODE_BITMAP(pnode), sizeof(__le64), 1);

    for (i = 1; i <= d; i++) {
        shim_update(PNODE_SORTED_KEY(new_pnode, i),
                    &PNODE_SORTED_VAL(pnode, i), &PNODE_SORTED_VAL(new_pnode, i));
    }

    i_key = resolve_key(PNODE_ANCHOR_KEY(new_pnode), &aux, &len);
    i_layer->insert(i_layer->pnode_index_struct[node], i_key, len, new_pnode);

	write_unlock(PNODE_LOCK(pnode));

	goto retry;
}

static void free_pnode(struct pnode* pnode) {
#if 0
	PMEMoid toid = pmemobj_oid(pnode);

	POBJ_FREE(&toid);
#endif
}

int pnode_remove(pkey_t key) {
	struct index_layer *i_layer = INDEX(bonsai);
	struct data_layer *d_layer = DATA(bonsai);
	struct pnode *pnode, *next_pnode;
	int h, pos, i, node;
	struct mptable* m;
    const void *i_key;
    uint64_t aux;
    size_t len;

	pnode = pnode_find_lowbound(key);

	assert(pnode);
 	assert(pkey_compare(key, PNODE_ANCHOR_KEY(pnode)) >= 0);

retry:
	write_lock(PNODE_LOCK(pnode));

    if (pnode->next && pkey_compare(pnode->next->anchor_key, key) <= 0) {
        /* pnode split between find_lowerbound and write_lock */
        next_pnode = pnode->next;
        write_unlock(PNODE_LOCK(pnode));
        pnode = next_pnode;
        goto retry;
    }

	h = hash8(key);
	pos = -1;

	for (i = 0; i < NUM_ENTRY_PER_PNODE; i++) {
		if (PNODE_FGPRT(pnode, i) == h) {
			if (pkey_compare(PNODE_KEY(pnode, i), key) == 0) {
				pos = i;
				break;
			}
		}
	}

	if (unlikely(pos == -1)) {
		assert(0);
		write_unlock(PNODE_LOCK(pnode));
		return -ENOENT;
	}

	pnode_sort_slot(pnode, pos, key, OP_REMOVE);
	__clear_bit((1ULL << pos), &PNODE_BITMAP(pnode));

#if 0
	if (unlikely(!pnode->slot[0])) {
        i_key = resolve_key(PNODE_ANCHOR_KEY(pnode_node0), &aux, &len);
		i_layer->remove(i_layer->pnode_index_struct, i_key, len);
		free_pnode(pnode);
	}
#endif

    write_unlock(PNODE_LOCK(pnode));
	return 0;
}

void sentinel_node_init(int node) {
	struct index_layer *i_layer = INDEX(bonsai);
    struct data_layer *d_layer = DATA(bonsai);
	pkey_t key = MIN_KEY;
	pval_t val = ULONG_MAX;
	pentry_t e = {key, val};
	struct pnode *pnode;
    const void *i_key;
    uint64_t aux;
    size_t len;
	int pos;

	/* DATA Layer: allocate a persistent node */
	pnode = alloc_pnode(node);

	pnode->anchor_key = key;
    d_layer->sentinel = pnode;

	write_lock(PNODE_LOCK(pnode));

	pos = find_unused_entry(PNODE_BITMAP(pnode));
	pnode->e[pos] = e;
    PNODE_FGPRT(pnode, pos) = hash8(key);
	pnode_sort_slot(pnode, pos, key, OP_INSERT);
	__set_bit(pos, &PNODE_BITMAP(pnode));

	bonsai_flush((void*)&pnode->e[pos], sizeof(pentry_t), 1);
	bonsai_flush((void*)&pnode->slot, NUM_ENTRY_PER_PNODE + 1, 0);
	bonsai_flush((void*)&PNODE_BITMAP(pnode), sizeof(__le64), 1);
	
	write_unlock(PNODE_LOCK(pnode));

    i_key = resolve_key(key, &aux, &len);
    i_layer->insert(i_layer->pnode_index_struct[node], i_key, len, pnode);

	bonsai_print("sentinel_node_init: node %d\n", node);
}

int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr) {
	uint8_t* slot;
	pval_t max_key = 0;
	int index = 1;
	
	slot = pnode->slot;
	while(index <= slot[0]) {
		if (likely(pkey_compare(PNODE_SORTED_KEY(pnode, index), high) <= 0
				&& pkey_compare(PNODE_SORTED_KEY(pnode, index), low) >= 0)) {
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

void print_pnode(struct pnode *pnode) {
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

void print_pnode_summary(struct pnode *pnode) {
	bonsai_print("pnode == bitmap: %016lx slot[0]: %d [%lu %lu] anchor{%lu}\n", PNODE_BITMAP(pnode), pnode->slot[0], PNODE_MIN_KEY(pnode), PNODE_MAX_KEY(pnode), PNODE_ANCHOR_KEY(pnode));
}

void dump_pnode_list(void (*printer)(struct pnode *pnode)) {
	struct data_layer *layer = DATA(bonsai);
    struct pnode *pnode;
	int pnode_sum = 0, key_sum = 0;

	bonsai_print("====================================================================================\n");

	for (pnode = layer->sentinel; pnode; pnode = pnode->next) {
        printer(pnode);

		key_sum += pnode->slot[0];
		pnode_sum ++;
	}

	bonsai_print("total pnode [%d]\n", pnode_sum);
	bonsai_print("data layer total entries [%d]\n", key_sum);
}

/*
 * data_layer_search_key: search @key in data layer, you must stop the world first.
 */
struct pnode* data_layer_search_key(pkey_t key) {
	struct data_layer *layer = DATA(bonsai);
    struct pnode *pnode;
	int i;
	
	for (pnode = layer->sentinel; pnode; pnode = pnode->next) {
		for (i = 1; i < pnode->slot[0]; i ++) {
			if (!pkey_compare(PNODE_SORTED_KEY(pnode, i), key)) {
				bonsai_print("data layer search key[%lu]: pnode %016lx key index %d, val address %016lx\n", key, pnode, pnode->slot[i], &pnode->e[pnode->slot[i]].v);
				print_pnode(pnode);
				return pnode;
			}
		}
	}

	bonsai_print("data layer find no such key[%lu]\n", key);

	return NULL;
}
