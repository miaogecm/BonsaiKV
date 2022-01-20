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

#include "mptable.h"
#include "oplog.h"
#include "pnode.h"
#include "common.h"
#include "bonsai.h"
#include "cpu.h"
#include "region.h"
#include "ordo.h"
#include "hash_set.h"

extern struct bonsai_info* bonsai;

#define NUM_MERGE_HASH_BUCKET		64

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

static struct mptable* init_one_mptable(int node) {
    struct mptable* mptable;

	mptable = (struct mptable*) numa_alloc_onnode(sizeof(struct mptable), node);

	/* initialize the hash table */
	hs_init(&mptable->hs);

    return mptable;
}

static void free_one_mptable(struct mptable *table) {
	numa_free(table, sizeof(struct mptable));
}

struct numa_table* numa_mptable_alloc(struct log_layer* layer) {
	struct numa_table* tables;
	int node;

	tables = malloc(sizeof(struct numa_table));
	
	for (node = 0; node < NUM_SOCKET; node ++) {
		tables->tables[node] = init_one_mptable(node);
	}
	
	tables->pnode = NULL;

	spin_lock(&layer->lock);
	list_add_tail(&tables->list, &layer->numa_table_list);
	spin_unlock(&layer->lock);

	return tables;
}

void numa_mptable_free(struct numa_table* tables) {
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		free_one_mptable(MPTABLE_NODE(tables, node));
	}

	free(tables);
}

int mptable_insert(struct numa_table* tables, int numa_node, int cpu, pkey_t key, pval_t value) {
	struct mptable* table = MPTABLE_NODE(tables, numa_node);
	struct oplog* log;
	int ret = 0, tid = get_tid();
#ifndef BONSAI_SUPPORT_UPDATE
	pval_t* addr;
	int node;
#endif

#ifdef BONSAI_SUPPORT_UPDATE
	log = oplog_insert(key, value, OP_INSERT, numa_node, cpu, table, tables->pnode);
	ret = hs_insert(&table->hs, tid, key, &log->o_kv.v);
#else
	for (node = 0; node < NUM_SOCKET; node ++) {
		table = MPTABLE_NODE(tables, node);
		addr = hs_lookup(&table->hs, tid, key);
		if (addr)
			return -EEXIST;
		else {
			log = oplog_insert(key, value, OP_INSERT, numa_node, cpu, table, table->pnode);
			hs_insert(&table->hs, tid, key, &log->o_kv.v);
		}
	}
#endif
	tables->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_insert: cpu[%d] <%lu, %lu>\n", cpu, key, value);

	return ret;
}

int mptable_update(struct numa_table* tables, int numa_node, int cpu, pkey_t key, pval_t* address) {
	struct mptable* mptable = MPTABLE_NODE(tables, numa_node);
	struct oplog* log;
	int tid = get_tid();
	pval_t value = *address;
#ifndef BONSAI_SUPPORT_UPDATE
	pval_t* addr;
	int node;
#endif

#ifdef BONSAI_SUPPORT_UPDATE
	log = oplog_insert(key, value, OP_INSERT, numa_node, cpu, mptable, tables->pnode);
	hs_insert(&mptable->hs, tid, key, &log->o_kv.v);
#else
	for (node = 0; node < NUM_SOCKET; node ++) {
		mptable = MPTABLE_NODE(tables, node);
		addr = hs_lookup(&mptable->hs, tid, key);
		if (addr)
			return -ENOENT;
		else {
			log = oplog_insert(key, *address, OP_INSERT, numa_node, cpu, mptable, mptable->pnode);
			hs_insert(&mptable->hs, tid, key, &log->o_kv.v);
		}
	}
#endif
	tables->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_update: cpu[%d] <%lu, %lu>\n", cpu, key, value);

	return 0;
}

int mptable_remove(struct numa_table* tables, int numa_node, int cpu, pkey_t key) {
	struct mptable* mptable = MPTABLE_NODE(tables, numa_node);
	int tid = get_tid();
	struct oplog* log;
	pval_t* addr;
	int node;
	unsigned long max_op_t = 0;
	int latest_op_type = -1, found_in_pnode = 0;
	

	for (node = 0; node < NUM_SOCKET; node ++) {
		mptable = MPTABLE_NODE(tables, node);
		addr = hs_lookup(&mptable->hs, tid, key);
		if (addr) {
			if (addr_in_log((unsigned long)addr)) {
				log = OPLOG(addr);
				if (ordo_cmp_clock(log->o_stamp, max_op_t) == ORDO_GREATER_THAN) {
					max_op_t = log->o_stamp;
					latest_op_type = log->o_type;
				}
			}
			else if (addr_in_pnode((unsigned long)addr)) {
				found_in_pnode = 1;
			}
		}
	}

	if (latest_op_type == OP_REMOVE || (max_op_t == 0 && !found_in_pnode)) {
		return -ENOENT;
	}

	log = oplog_insert(key, 0, OP_REMOVE, numa_node, cpu, mptable, tables->pnode);
	hs_insert(&mptable->hs, tid, key, &log->o_kv.v);

	tables->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_remove: cpu[%d] %lu\n", cpu, key);

	return 0;
}

int mptable_lookup(struct numa_table* mptables, pkey_t key, int cpu, pval_t* val) {
	int node, tid = get_tid();
	struct oplog* insert_logs[NUM_SOCKET];
	struct oplog* log = NULL;
	struct mptable *table;
	pval_t *addr, *master_node_addr, *self_node_addr;
	unsigned long max_insert_t = 0, max_remove_t = 0;
	int n_insert = 0, n_remove = 0, numa_node = get_numa_node(cpu);
	int max_insert_index;
	void* map_addrs[NUM_SOCKET];
	struct pnode* pnode;

	bonsai_debug("mptable_lookup: cpu[%d] %lu\n", cpu, key);
	
	for (node = 0; node < NUM_SOCKET; node ++) {
		table = MPTABLE_NODE(mptables, node);
		addr = hs_lookup(&table->hs, tid, key);
		map_addrs[node] = addr;

		if (addr_in_log((unsigned long)addr)) {
			/* case I: mapping table entry points to an oplog */
			log = OPLOG(addr);
			switch (log->o_type) {
			case OP_INSERT:
				if (ordo_cmp_clock(log->o_stamp, max_insert_t) == ORDO_GREATER_THAN) {
					max_insert_t = log->o_stamp;
					max_insert_index = node;
				}
				insert_logs[node] = log;
				n_insert++;
				break;
			case OP_REMOVE:
				if (ordo_cmp_clock(log->o_stamp, max_remove_t) == ORDO_GREATER_THAN) {
					max_remove_t = log->o_stamp;
				}
				n_remove++;
				break;
			}
		} else if (addr_in_pnode((unsigned long)addr)) {
			/* case II: mapping table entry points to a pnode entry */
			if (node == 0)
				master_node_addr = addr;
			else if (node == numa_node)
				self_node_addr = addr;
		} else {
			/* case III: mapping table entry is NULL */
			if (node == numa_node)
				self_node_addr = addr;
		}
	}

	if (n_insert > 0) {
		if (ordo_cmp_clock(max_remove_t, max_insert_t) == ORDO_GREATER_THAN) {
			return -ENOENT;
		} else {
			log = insert_logs[max_insert_index];
			*val = log->o_kv.v;
			return 0;
		} 
	} else if (n_insert == 0) {
#ifdef BONSAI_SUPPORT_UPDATE
		if (n_remove > 0)
			return -ENOENT;
		else {
			addr = map_addrs[numa_node];
			if (addr) {
				*val = *addr;
				return 0;
			} else {
				if (map_addrs[0]) {
					/* pull latest value */
					table = MPTABLE_NODE(mptables, numa_node);
					hs_insert(&table->hs, tid, key, map_addrs[0]);
					
					/* migrate this value */
					pnode = (struct pnode*)log->o_addr;
					addr = pnode_numa_move(pnode, key, numa_node);
					*val = *addr;
					return 0;
				}
			}	
		}
#else
		numa_node = numa_node();
		addr = map_addrs[numa_node];
		if (addr) {
			*val = *addr;
			return 0;
		} else {
			if (map_addrs[0]) {
				/* pull latest value */
				table = MPTABLE_NODE(mptables, numa_node);
				hs_insert(&table->hs, tid, key, map_addrs[0]);

				/* migrate this value */
				pnode = (struct pnode*)log->o_addr;
				addr = pnode_numa_move(pnode, key, numa_node);
				*val = *addr;
				return 0;
			}
		}
#endif
	}

	return -ENOENT;
}

void mptable_update_addr(struct numa_table* tables, int numa_node, pkey_t key, pval_t* addr) {
	struct mptable *table = tables->tables[numa_node];
	int tid = get_tid();

	hs_insert(&table->hs, tid, key, addr);
}

void mptable_split(struct numa_table* old_table, struct pnode* new_pnode, struct pnode* old_pnode) {	
	int tid = get_tid(), i, node, N = new_pnode->slot[0];
	struct numa_table* new_table;
	struct mptable *m;
	struct index_layer* i_layer = INDEX(bonsai);
	pkey_t key;
	pval_t *addr;
	uint8_t* slot = new_pnode->slot;
	
	new_table = numa_mptable_alloc(LOG(bonsai));
	new_pnode->table = new_table;
	new_table->pnode = new_pnode;

	for (i = 1; i <= N; i ++) {
		key = new_pnode->e[slot[i]].k;
		for (node = 0; node < NUM_SOCKET; node ++) {
			m = MPTABLE_NODE(old_table, node);
			if ((addr = hs_lookup(&m->hs, tid, key)) != NULL) {
				if (addr_in_pnode((unsigned long)addr)) {
					hs_remove(&m->hs, tid, key);
					hs_insert(&new_table->tables[node]->hs, tid, key, &new_pnode->e[slot[slot[0]]].v);
					//bonsai_debug("mptable_split [%d]: <%lu %016lx>\n", i, key, &pnode->e[pnode->slot[0]].v);
				}
			}		
		}
	}
	
	i_layer->insert(i_layer->index_struct, pnode_anchor_key(old_pnode), old_table);
	i_layer->insert(i_layer->index_struct, pnode_anchor_key(new_pnode), new_table);
}

static void merge_one_log(struct hbucket* merge_buckets, pval_t* val, pkey_t low, pkey_t high, pkey_t* max_key) {
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
	hlist_add_head(&e->node, &merge_buckets[simple_hash(e->kv->k)].head);
}

int __compare(const void* a, const void* b) {
	return key_cmp((*(pentry_t**)a)->k, (*(pentry_t**)b)->k);
}

#define MAX_LEN		100001
static int __mptable_scan(struct numa_table* table, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr_key) {
	struct hbucket merge_buckets[NUM_MERGE_HASH_BUCKET];
	struct bucket_list **buckets, *bucket;
	struct hash_set* hs;
	struct mptable *m;
	segment_t* p_segment;
	struct ll_node *head, *curr;
	struct hlist_node *hnode, *n_hnode;
	int node, i, j;
	struct oplog *l;
	scan_merge_ent* e;
	pentry_t* arr[MAX_LEN];
	pkey_t max_key = 0;

	memset(merge_buckets, 0, sizeof(struct hbucket) * NUM_MERGE_HASH_BUCKET);
	/* 1. scan mapping table, merge logs */
	for (node = 0; node < NUM_SOCKET; node ++) {
		m =  table->tables[node];
		hs = &m->hs;
		
		for (i = 0; i < MAIN_ARRAY_LEN; i++) {
			p_segment = hs->main_array[i];
        	if (p_segment == NULL)
            	continue;

			for (j = 0; j < SEGMENT_SIZE; j++) {
				buckets = (struct bucket_list**)p_segment;
				bucket = buckets[j];
				if (bucket == NULL) 
                	continue;
				
            	head = &(bucket->bucket_sentinel.ll_head);
				curr = GET_NODE(head->next);
				while (curr) {
                	if (is_sentinel_key(curr->key)) {
                   	 	break;
                	} else {
						merge_one_log(merge_buckets, (void*)curr->val, low, high, &max_key);
                	}
                	curr = GET_NODE(STRIP_MARK(curr->next));
            	} 
			}
		}
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

	return n;
}

int mptable_scan(struct numa_table* table, pkey_t low, pkey_t high, pval_t* result) {
	struct pnode* pnode;
	pkey_t curr = low;
	int n = 0, ret = 0;

	if (low > high) {
		return 0;
	}

	pnode = table->pnode;
	while (key_cmp(curr, high) <= 0) {
		if (pnode->stale == PNODE_DATA_CLEAN) {
			n = scan_one_pnode(pnode, n, low, high, result, &curr);	
		} else {
			n = __mptable_scan(table, n, low, high, result, &curr);
		}
		table = list_next_entry(table, list);
		pnode = table->pnode;
		ret += n;	
	}

	return ret;
}
