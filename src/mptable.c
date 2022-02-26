/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <numa.h>
#include <assert.h>
#include <limits.h>
#include <libpmemobj.h>

#include "mptable.h"
#include "oplog.h"
#include "pnode.h"
#include "common.h"
#include "bonsai.h"
#include "cpu.h"
#include "region.h"
#include "ordo.h"
#include "hash_set.h"
#include "hash.h"
#include "per_node.h"
#include "long_key.h"

extern struct bonsai_info* bonsai;

extern void kv_print(void* index_struct);

typedef struct {
	int flag; /* 0:log; 1:pnode */
	pentry_t* kv;
	struct hlist_node node;
} scan_merge_ent;

int addr_in_log(unsigned long addr) {
	struct log_layer* layer = LOG(bonsai);
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		if ((unsigned long)layer->pmem_addr[node] <= addr && 
			addr <= ((unsigned long)layer->pmem_addr[node] + LOG_REGION_SIZE))
			return 1;
	}
	
	return 0;
}

int addr_in_pnode(unsigned long addr) {
	struct data_layer* layer = DATA(bonsai);
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		if ((layer->region[node].start <= addr) &&
			(addr <= layer->region[node].start + DATA_REGION_SIZE))
			return 1;
	}
	
	return 0;
}

struct mptable* mptable_alloc(struct log_layer* layer) {
    struct mptable *table, *cur;
	int node;

    table = per_node_obj_alloc(&layer->mptable_arena);
    for_each_obj(&layer->mptable_arena, node, cur, table) {
        cur->hs.small = 1;
        hs_init(&cur->hs);
        rwlock_init(&cur->rwlock);
        cur->pnode = NULL;
        cur->forward = NULL;
    }

	spin_lock(&layer->table_lock);
	list_add_tail(&table->list, &layer->mptable_list);
	spin_unlock(&layer->table_lock);

	return table;
}

void mptable_free(struct log_layer* layer, struct mptable *table) {
    per_node_obj_free(&layer->mptable_arena, table);
}

static inline int in_target_table(struct mptable *table, pkey_t key) {
    pkey_t lowerbound = ACCESS_ONCE(table->lowerbound);
    return key > lowerbound || lowerbound == ULONG_MAX;
}

#ifdef LONG_KEY
static uint64_t alloc_long_key(struct data_layer* layer, pkey_t key, uint16_t k_len) {
	PMEMobjpool* pop = layer->key_pop;
	TOID(struct long_key) toid;
	struct long_key* root;

	POBJ_ALLOC(pop, &toid, struct long_key, sizeof(struct pnode), NULL, NULL);

	root = pmemobj_direct(toid.oid);
	
	root->len = k_len;
	pmemobj_persist(pop, &root->len, sizeof(uint16_t));
	pmemobj_memcpy_persist(pop, root->key, key, k_len);

	return toid.oid.off;
}
#endif

#define OFF_CHECK_MASK	0xffff000000000000

int mptable_insert(int numa_node, int cpu, pkey_t key, uint16_t k_len, pval_t value) {
	struct mptable* table;
	struct index_layer* i_layer = INDEX(bonsai);
	struct oplog* log;
	int ret = 0, tid = get_tid();
	uint64_t __key = key;
#ifndef BONSAI_SUPPORT_UPDATE
	pval_t* addr;
	int node;
#endif

    /* TODO: It's dangerous to bind oplog entry directly with pnode! */
#ifdef LONG_KEY
	uint64_t off;
	struct data_layer* d_layer = DATA(bonsai);

	off = alloc_long_key(d_layer, key, k_len);

	assert(!(off & OFF_CHECK_MASK));
	__key = (off << KEY_LEN_BITS) & k_len;
#endif
	log = oplog_insert(__key, value, OP_INSERT, numa_node, cpu, NULL);

#ifdef BONSAI_SUPPORT_UPDATE
retry:
    table = (struct mptable*)i_layer->lookup(i_layer->index_struct, __key);
	assert(table);
    table = node_ptr(&LOG(bonsai)->mptable_arena, table, numa_node);

	read_lock(&table->rwlock);
	ret = hs_update(&table->hs, tid, __key, &log->o_kv.v);
	read_unlock(&table->rwlock);

    
    if (unlikely(!in_target_table(table, key))) {
        goto retry;
    }
#else
	for (node = 0; node < NUM_SOCKET; node ++) {
		table = MPTABLE_NODE(tables, node);
		read_lock(&table->rwlock);
		addr = hs_lookup(&table->hs, tid, key);
		if (addr) {
			read_unlock(&table->rwlock);
			return -EEXIST;
		}
		read_unlock(&table->rwlock);
	}
	table = MPTABLE_NODE(tables, numa_node);
	log = oplog_insert(key, value, OP_INSERT, numa_node, cpu, table, table->pnode);
	hs_insert(&table->hs, tid, key, &log->o_kv.v);
#endif
	table->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_insert: cpu[%d] <%lu, %lu>\n", cpu, key, value);

	return ret;
}

#if 1
int mptable_update(int numa_node, int cpu, pkey_t key, uint16_t k_len, pval_t *address) {
    assert(0);
}
#endif

static int __mptable_remove(struct mptable* table, int numa_node, int cpu, pkey_t key,
		int* latest_op_type, int* found_in_pnode) {
	unsigned long max_op_t = 0;
	struct oplog* log;
	pval_t *addr;
	int tid = get_tid();
	struct mptable* m;
	int node, found = 0;
    struct log_layer *layer = LOG(bonsai);

    for_each_obj(&layer->mptable_arena, node, m, table) {
		addr = hs_lookup(&m->hs, tid, key);
		if (addr) {
			if (addr_in_log((unsigned long)addr)) {
				log = OPLOG(addr);
				if (ordo_cmp_clock(log->o_stamp, max_op_t) == ORDO_GREATER_THAN) {
					max_op_t = log->o_stamp;
					*latest_op_type = log->o_type;
					found = 1;
					if (log->o_type == OP_REMOVE) 
						bonsai_print("log %016lx type %d <%lu %lu> %lu\n", log, log->o_type, log->o_kv.k, log->o_kv.v, key);
				}
			} else if (addr_in_pnode((unsigned long)addr)) {
				*found_in_pnode = 1;
				found = 1;
			} else {
				perror("invalid address\n");
				assert(0);
			}
		}
	}

	return found;
}

static void free_long_key(pkey_t key) {
	struct data_layer* layer = &(bonsai->d_layer);
	PMEMoid oid;
	char* addr;
	
	addr = (key >> KEY_LEN_BITS) + layer->key_start;
	oid = pmemobj_oid(addr);
	
	POBJ_FREE(&oid);
}

int mptable_remove(int numa_node, int cpu, pkey_t key, uint16_t k_len) {
    struct mptable *table;
	struct index_layer* i_layer = INDEX(bonsai);
	int latest_op_type = OP_NONE, found_in_pnode = 0;
	struct oplog* log;
	int tid = get_tid();
    struct log_layer *layer = LOG(bonsai);
	uint64_t __key = key;

#ifdef LONG_KEY
	uint64_t off;
	struct data_layer* d_layer = DATA(bonsai);

	off = alloc_long_key(d_layer, key, k_len);

	assert(!(off & OFF_CHECK_MASK));
	__key = (off << KEY_LEN_BITS) & k_len;
#endif
    /* TODO: It's dangerous to bind oplog entry directly with pnode! */
	log = oplog_insert(__key, 0, OP_REMOVE, numa_node, cpu, NULL);


retry:
	table = (struct mptable*)i_layer->lookup(i_layer->index_struct, key);
	assert(table);
    table = node_ptr(&layer->mptable_arena, table, numa_node);
	
	if (unlikely(!__mptable_remove(table, numa_node, cpu, __key, &latest_op_type, &found_in_pnode)
				&& table->forward)) {
		/* search forward table */
		__mptable_remove(table->forward, numa_node, cpu, __key, &latest_op_type, &found_in_pnode);
	}

	if (latest_op_type == OP_REMOVE) {
		bonsai_print("latest_op_type == OP_REMOVE [%lu] %d %d\n", key, latest_op_type, found_in_pnode);
		check_mptable();
		return -ENOENT;
	}

	if (latest_op_type == OP_NONE && !found_in_pnode) {
        /* Maybe not in this table now. */
        if (unlikely(!in_target_table(table, key))) {
            goto retry;
        }
		bonsai_print("no such entry [%lu]\n", key);
		return -ENOENT;
	}

	hs_update(&table->hs, tid, key, &log->o_kv.v);

    if (unlikely(!in_target_table(table, key))) {
        goto retry;
    }

	free_long_key(log->o_kv.k);

	table->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_remove: cpu[%d] %lu\n", cpu, key);

	return 0;
}

pval_t* __mptable_lookup(struct mptable* mptable, pkey_t key, int cpu) {
	int node, tid = get_tid();
	struct mptable *m;
	pval_t *addr, *ret = NULL;
	//pval_t *master_node_addr, *self_node_addr;
	//int numa_node = get_numa_node(cpu);
	unsigned long max_op_t = 0;
	struct oplog *log;
    struct log_layer *layer = LOG(bonsai);

    for_each_obj(&layer->mptable_arena, node, m, mptable) {
		addr = hs_lookup(&m->hs, tid, key);

		if (addr_in_log((unsigned long)addr)) {
			/* case I: mapping table entry points to an oplog */
			log = OPLOG(addr);
			if (ordo_cmp_clock(log->o_stamp, max_op_t) == ORDO_GREATER_THAN) {
				max_op_t = log->o_stamp;
				ret = &log->o_kv.v;
			}
		} else if (addr_in_pnode((unsigned long)addr)) {
			/* case II: mapping table entry points to a pnode entry */
			#if 0
			if (node == 0)
				master_node_addr = addr;
			else if (node == numa_node) {
				self_node_addr = addr;
			}
			#endif
			if (!ret)
				ret = addr;
		} else {
			/* case III: mapping table entry is NULL */
			/*
			if (node == numa_node)
				self_node_addr = addr;
			*/
		}
	}

	return ret;
}

int mptable_lookup(int numa_node, pkey_t key, uint16_t k_len, int cpu, pval_t *val) {
	struct index_layer* i_layer = INDEX(bonsai);
	int tid = get_tid();
	struct pnode* pnode;
	void* map_addrs[NUM_SOCKET];
	struct mptable *table;
	pval_t *addr = NULL;
	struct oplog* log;
    struct log_layer *layer = LOG(bonsai);
	uint64_t __key = key;

#ifdef LONG_KEY
	uint64_t off;
	struct data_layer* d_layer = DATA(bonsai);

	off = alloc_long_key(d_layer, key, k_len);

	assert(!(off & OFF_CHECK_MASK));
	__key = (off << KEY_LEN_BITS) & k_len;
#endif

	bonsai_debug("mptable_lookup: cpu[%d] %lu\n", cpu, key);

retry:
    table = (struct mptable*)i_layer->lookup(i_layer->index_struct, __key);
    table = node_ptr(&layer->mptable_arena, table, numa_node);

	addr = __mptable_lookup(table, __key, cpu);
	
	if (!addr && table->forward) {
		addr = __mptable_lookup(table->forward, __key, cpu);
	}
	
	if (addr_in_log((unsigned long)addr)) {
		log = OPLOG(addr);
		switch (log->o_type) {
		case OP_INSERT:
			*val = log->o_kv.v;
			return 0;
		case OP_REMOVE:
			return -ENOENT;
		default:
			perror("invalid operation type\n");
			printf("type: %d\n", log->o_type);
		}
	} else if (addr_in_pnode((unsigned long)addr)) {
		if (addr) {
			*val = *addr;
			return 0;
		} else {
			/* FIXME */
			assert(0);
#if 0
			if (map_addrs[0]) {
				/* pull latest value */
				m = MPTABLE_NODE(tables, numa_node);
				hs_insert(&m->hs, tid, key, map_addrs[0]);
				
				/* migrate this value */
				//pnode = (struct pnode*)log->o_addr;
				addr = pnode_numa_move(pnode, key, numa_node, map_addrs[0]);
				*val = *addr;
				return 0;
			}
#endif
		}
	} else {
        if (unlikely(!in_target_table(table, key))) {
            goto retry;
        }

		// stop_the_world();
		// bonsai_print("***************************key: %lu; addr: %016lx*****************************\n", key, addr);
		// printf("table anchor_key: %016lx; forward: %016lx\n", table->pnode, table->forward ? table->forward->pnode : NULL);
		// mptable_search_key(key);
		// data_layer_search_key(key);
		// dump_pnode_list_summary();
		// assert(0);
	}

	return -ENOENT;
}

void mptable_update_addr(struct mptable* table, pkey_t key, pval_t* addr) {
	int tid = get_tid();
	hs_update(&table->hs, tid, key, addr);
}

void mptable_split(struct mptable* old_table, struct pnode* new_pnode, struct pnode* mid_pnode, pkey_t avg_key) {
	int node;
	struct mptable *new_table, *mid_table, *cur;
	struct mptable *old_m, *new_m, *mid_m;
	struct index_layer* i_layer = INDEX(bonsai);
    pkey_t lowerbound;
    struct log_layer *layer = LOG(bonsai);

	/* 1. allocate a new mapping table */
	new_table = mptable_alloc(LOG(bonsai));
	new_pnode->table = new_table;
    for_each_obj(&layer->mptable_arena, node, cur, new_table) {
        cur->pnode = new_pnode;
        cur->forward = old_table;
        cur->lowerbound = old_table->lowerbound;
    }

	if (mid_pnode) {
		mid_table = mptable_alloc(LOG(bonsai));
		mid_pnode->table = mid_table;
        for_each_obj(&layer->mptable_arena, node, cur, mid_table) {
            cur->pnode = mid_pnode;
            cur->forward = old_table;
            cur->lowerbound = new_pnode->anchor_key;
        }
	}

	/* 2. update the index layer */
	i_layer->insert(i_layer->index_struct, PNODE_ANCHOR_KEY(new_pnode), new_table);

	if (mid_pnode) {
		i_layer->insert(i_layer->index_struct, avg_key, mid_table);
	}

    lowerbound = (mid_pnode ? mid_pnode : new_pnode)->anchor_key;
    for_each_obj(&layer->mptable_arena, node, cur, old_table) {
        cur->lowerbound = lowerbound;
    }
    /*
     * When entry migration is running, we must ensure that all threads insert
     * into the correct mptable.
     */
    smp_mb();

	/* 3. copy entries from @old_table to @new_table */
	for (node = 0; node < NUM_SOCKET; node ++) {
		old_m = node_ptr(&layer->mptable_arena, old_table, node);
		new_m = node_ptr(&layer->mptable_arena, new_table, node);
		if (mid_pnode) {
			mid_m = node_ptr(&layer->mptable_arena, mid_table, node);
			hs_scan_and_split(&old_m->hs, &new_m->hs, &mid_m->hs, PNODE_MAX_KEY(new_pnode), avg_key, new_pnode, mid_pnode);
		} else {
			hs_scan_and_split(&old_m->hs, &new_m->hs, NULL, PNODE_MAX_KEY(new_pnode), avg_key, new_pnode, NULL);
		}
	}

    for_each_obj(&layer->mptable_arena, node, cur, new_table) {
        cur->forward = NULL;
    }
	
	if (mid_pnode) {
        for_each_obj(&layer->mptable_arena, node, cur, mid_table) {
		    cur->forward = NULL;
        }
	}
}

static void merge_one_log(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	pval_t* val = node->val;
	struct hbucket* merge_buckets = (struct hbucket*)arg1;
	pkey_t low = (pkey_t)arg2, high = (pkey_t)arg3;
	pkey_t* max_key = (pkey_t*)arg4;
	struct hbucket* bucket;
	struct hlist_node* hnode;
	scan_merge_ent* e;
	struct oplog *l1, *l2;
	pkey_t* key = (pkey_t*)((unsigned long)val - sizeof(pval_t));
	
	*max_key = key_cmp(*max_key, *key) < 0 ? *key: *max_key;

	if (unlikely(key_cmp(*key, high) > 0 || key_cmp(*key, low) < 0)) {
		return;
	}

	bucket = &merge_buckets[*key % NUM_MERGE_HASH_BUCKET];
	hlist_for_each_entry(e, hnode, &bucket->head, node) {
		/* reach the end */
		// if (key_cmp(e->kv->k, high) > 0) {
		// 	return;
		// }

		/* try to merge */
		if (key_cmp(e->kv->k, *key) == 0) {
			if (addr_in_log((unsigned long)val)) {
				l1 = (struct oplog*)((unsigned long)val + sizeof(pval_t) - sizeof(struct oplog));
				if (addr_in_log((unsigned long)e->kv)) {
					l2 = (struct oplog*)((unsigned long)e->kv + sizeof(pentry_t) - sizeof(struct oplog));
					if (ordo_cmp_clock(l1->o_stamp, l2->o_stamp) == ORDO_GREATER_THAN) {
						e->kv = &l1->o_kv;
						return;
					}		
				} else {
					e->kv = &l1->o_kv;
					return;
				}
			}
		}
	}
	
	e = malloc(sizeof(scan_merge_ent));
	e->kv = (pentry_t*)((unsigned long)val - sizeof(pkey_t));
	hlist_add_head(&e->node, &merge_buckets[p_hash(e->kv->k)].head);
}

int __compare(const void* a, const void* b) {
	return key_cmp((*(pentry_t**)a)->k, (*(pentry_t**)b)->k);
}

#define MAX_LEN		(16 * 1024 * 1024)
static int __mptable_scan(struct mptable* table, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr_key) {
	struct hbucket* merge_buckets;
	struct mptable *m;
	struct hlist_node *hnode, *n_hnode;
	int node, i, j;
	struct oplog *l;
	scan_merge_ent* e;
	pentry_t** arr;
	pkey_t max_key = 0;
    struct log_layer *layer = LOG(bonsai);

	merge_buckets = malloc(sizeof(struct hbucket) * NUM_MERGE_HASH_BUCKET);
	arr = malloc(sizeof(pentry_t*) * MAX_LEN);

    for (i = 0; i < NUM_MERGE_HASH_BUCKET; i++) {
        INIT_HLIST_HEAD(&merge_buckets[i].head);
    }

	/* 1. scan mapping table, merge logs */
    for_each_obj(&layer->mptable_arena, node, m, table) {
		hs_scan_and_ops(&m->hs, merge_one_log, (void*)merge_buckets, (void*)low, (void*)high, (void*)&max_key, (void*)NULL);
    }

	/* 2. scan merge hash table and copy */
	for (i = 0, j = 0; i < NUM_MERGE_HASH_BUCKET; i ++) {
		hlist_for_each_entry_safe(e, hnode, n_hnode, &merge_buckets[i].head, node) {
			if (addr_in_log((unsigned long)e->kv)) {
				l = (struct oplog*)((unsigned long)e->kv + sizeof(pentry_t) - sizeof(struct oplog));
				if (l->o_type & OP_REMOVE)
					continue;
			}
			arr[j++] = e->kv;
			
			hlist_del(&e->node);
			free(e);
		}
	}

	/* 3. sort */
	qsort((void*)arr, (size_t)j, sizeof(pentry_t*), __compare);

	/* 4. copy result */
	for (i = 0; i < j; i ++)
		result[n++] = arr[i]->v;

	*curr_key = max_key;

	free(merge_buckets);
	free(arr);

	return n;
}

int mptable_scan(struct mptable* table, pkey_t low, uint16_t lo_len, pkey_t high, uint16_t hi_len, pval_t* result) {
	struct pnode* pnode;
	pkey_t curr;
	int n = 0, ret = 0;
	uint64_t __high = high, __low = low;

#ifdef LONG_KEY
	uint64_t off;
	struct data_layer* d_layer = DATA(bonsai);

	off = alloc_long_key(d_layer, low, lo_len);

	assert(!(off & OFF_CHECK_MASK));
	__low = (off << KEY_LEN_BITS) & lo_len;

	off = alloc_long_key(d_layer, high, hi_len);

	assert(!(off & OFF_CHECK_MASK));
	__high = (off << KEY_LEN_BITS) & hi_len;
#endif

	if (key_cmp(__low, __high) > 0) {
		return 0;
	}

	curr = __low;
	pnode = table->pnode;
	while (key_cmp(curr, __high) <= 0) {
		if (pnode->stale == PNODE_DATA_CLEAN) {
			n = scan_one_pnode(pnode, n, __low, __high, result, &curr);	
		} else {
			n = __mptable_scan(table, n, __low, __high, result, &curr);
		}
		pnode = list_next_entry(pnode, list);
		table = pnode->table;
		ret += n;	
	}

	return ret;
}

void mptable_search_key(pkey_t key) {
	struct log_layer* layer = LOG(bonsai);
	struct mptable *m, *table;
	int node;

	spin_lock(&layer->table_lock);
	list_for_each_entry(table, &layer->mptable_list, list) {
        for_each_obj(&layer->mptable_arena, node, m, table) {
			hs_scan_and_ops(&m->hs, hs_search_key, (void*)key, NULL, NULL, NULL, NULL);
        }
	}
	spin_unlock(&layer->table_lock);

	bonsai_print("numa table find no such key[%lu]\n", key);
}

void dump_mptable() {
	struct log_layer* layer = LOG(bonsai);
	struct mptable *m, *table;
	int node;

	bonsai_print("====================================DUMP MPTABLE================================================\n");

	spin_lock(&layer->table_lock);
	list_for_each_entry(table, &layer->mptable_list, list) {
        for_each_obj(&layer->mptable_arena, node, m, table) {
			bonsai_print("Node[%d] table %016lx small:%d \n", node, m, m->hs.small);
			hs_scan_and_ops(&m->hs, hs_print_entry, NULL, NULL, NULL, NULL, NULL);
			bonsai_print("\n");
        }
	}
	spin_unlock(&layer->table_lock);
}

void check_mptable() {
	struct log_layer* layer = LOG(bonsai);
	struct mptable *m, *table;
	int node;

	bonsai_print("====================================CHECK MPTABLE================================================\n");

	spin_lock(&layer->table_lock);
	list_for_each_entry(table, &layer->mptable_list, list) {
        for_each_obj(&layer->mptable_arena, node, m, table) {
			hs_scan_and_ops(&m->hs, hs_check_entry, NULL, NULL, NULL, NULL, NULL);
		}
	}
	spin_unlock(&layer->table_lock);
}

void stat_mptable() {
	struct log_layer* layer = LOG(bonsai);
	struct mptable *m, *table;
	int node, i = 0, sum = 0, total = 0;
	pkey_t max = 0;

	bonsai_print("====================================COUNT MPTABLE================================================\n");

	spin_lock(&layer->table_lock);
	list_for_each_entry(table, &layer->mptable_list, list) {
        for_each_obj(&layer->mptable_arena, node, m, table) {
			hs_scan_and_ops(&m->hs, hs_count_entry, (void*)&sum, (void*)&max, NULL, NULL, NULL);
			bonsai_print("Mapping Table[%d] total entries: %d max: %lu\n", i++, sum, max);
		}
		total += sum;
		max = 0; sum = 0;
	}
	spin_unlock(&layer->table_lock);

	bonsai_print("log layer total entries: %d\n", total);
}
