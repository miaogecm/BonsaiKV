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
#include "hash.h"

extern struct bonsai_info* bonsai;

extern void kv_print(void* index_struct);

#define NUM_MERGE_HASH_BUCKET		(16 * 1024 * 1024)

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

	rwlock_init(&mptable->rwlock);

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
	tables->forward = NULL;

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
	read_lock(&table->rwlock);
	ret = hs_update(&table->hs, tid, key, &log->o_kv.v);
	read_unlock(&table->rwlock);
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
	tables->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_insert: cpu[%d] <%lu, %lu>\n", cpu, key, value);

	return ret;
}

#if 1
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
	read_lock(&mptable->rwlock);
	hs_update(&mptable->hs, tid, key, &log->o_kv.v);
	read_unlock(&mptable->rwlock);
#else
	for (node = 0; node < NUM_SOCKET; node ++) {
		mptable = MPTABLE_NODE(tables, node);
		read_lock(&mptable->rwlock);
		addr = hs_lookup(&mptable->hs, tid, key);
		if (addr) {
			read_unlock(&mptable->rwlock);
			return -ENOENT;
		}
		else {
			log = oplog_insert(key, *address, OP_INSERT, numa_node, cpu, mptable, mptable->pnode);
			hs_insert(&mptable->hs, tid, key, &log->o_kv.v);
			read_unlock(&mptable->rwlock);
		}
	}
#endif
	tables->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_update: cpu[%d] <%lu, %lu>\n", cpu, key, value);

	return 0;
}
#endif

static int __mptable_remove(struct numa_table* tables, int numa_node, int cpu, pkey_t key, 
		int* latest_op_type, int* found_in_pnode) {
	unsigned long max_op_t = 0;
	struct oplog* log;
	pval_t val, *addr;
	int tid = get_tid();
	struct mptable* m;
	int node, found = 0;
		
	for (node = 0; node < NUM_SOCKET; node ++) {
		m = MPTABLE_NODE(tables, node);
		addr = hs_lookup(&m->hs, tid, key);
		if (addr) {
			if (addr_in_log((unsigned long)addr)) {
				log = OPLOG(addr);
				if (ordo_cmp_clock(log->o_stamp, max_op_t) == ORDO_GREATER_THAN) {
					max_op_t = log->o_stamp;
					*latest_op_type = log->o_type;
					found = 1;
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

int mptable_remove(struct numa_table* tables, int numa_node, int cpu, pkey_t key) {
	int latest_op_type = OP_NONE, found_in_pnode = 0;
	struct mptable* m = MPTABLE_NODE(tables, numa_node);
	struct oplog* log;
	int tid = get_tid();
	
	if (unlikely(!__mptable_remove(tables, numa_node, cpu, key, &latest_op_type, &found_in_pnode) 
				&& tables->forward)) {
		/* search forward table */
		__mptable_remove(tables->forward, numa_node, cpu, key, &latest_op_type, &found_in_pnode);
	}

	if (latest_op_type == OP_REMOVE) {
		perror("latest_op_type == OP_REMOVE\n");
		return -ENOENT;
	}

	if (latest_op_type == OP_NONE && !found_in_pnode) {
		perror("no such entry\n");
		return -ENOENT;
	}

	log = oplog_insert(key, 0, OP_REMOVE, numa_node, cpu, m, tables->pnode);
	hs_update(m, tid, key, &log->o_kv.v);

	tables->pnode->stale = PNODE_DATA_STALE;

	bonsai_debug("mptable_remove: cpu[%d] %lu\n", cpu, key);

	return 0;
}

pval_t* __mptable_lookup(struct numa_table* mptables, pkey_t key, int cpu) {
	int node, tid = get_tid();
	struct mptable *m;
	pval_t *addr, *ret = NULL, *master_node_addr, *self_node_addr;
	int numa_node = get_numa_node(cpu);
	unsigned long max_op_t = 0;
	struct oplog *log;

	for (node = 0; node < NUM_SOCKET; node ++) {
		m = MPTABLE_NODE(mptables, node);
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
			if (node == 0)
				master_node_addr = addr;
			else if (node == numa_node) {
				self_node_addr = addr;
			}
			if (!ret)
				ret = addr;
		} else {
			/* case III: mapping table entry is NULL */
			if (node == numa_node)
				self_node_addr = addr;
		}
	}

	return ret;
}

int mptable_lookup(struct numa_table* tables, pkey_t key, int cpu, pval_t* val) {
	int tid = get_tid(), numa_node = get_numa_node(cpu);
	struct pnode* pnode;
	void* map_addrs[NUM_SOCKET];
	struct mptable *m;
	pval_t *addr = NULL;
	struct oplog* log;

	bonsai_debug("mptable_lookup: cpu[%d] %lu\n", cpu, key);

	addr = __mptable_lookup(tables, key, cpu);
	
	if (!addr && tables->forward) {
		addr = __mptable_lookup(tables->forward, key, cpu);
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
		}
	} else if (addr_in_pnode((unsigned long)addr)) {
		if (addr) {
			*val = *addr;
			return 0;
		} else {
			/* FIXME */
			assert(0);
			if (map_addrs[0]) {
				/* pull latest value */
				m = MPTABLE_NODE(tables, numa_node);
				hs_insert(&m->hs, tid, key, map_addrs[0]);
				
				/* migrate this value */
				pnode = (struct pnode*)log->o_addr;
				addr = pnode_numa_move(pnode, key, numa_node);
				*val = *addr;
				return 0;
			}
		}
	} else {
		bonsai_print("************invalid address %016lx %lu***********\n", addr, key);
		print_pnode_summary(tables->pnode);
		bonsai_print("%016lx tables: %016lx hs: %016lx\n", __mptable_lookup(tables, key, cpu), tables, &tables->tables[0]->hs);
		if (tables->forward)
			bonsai_print("%016lx\n", __mptable_lookup(tables->forward, key, cpu));
		stop_the_world();
		numa_table_search_key(key);
		log_layer_search_key(cpu, key);
		data_layer_search_key(key);
		index_layer_dump();
		//dump_pnode_list();
		//dump_numa_table();
		assert(0);
	}

	return -ENOENT;
}

void mptable_update_addr(struct numa_table* tables, int numa_node, pkey_t key, pval_t* addr) {
	struct mptable *table = tables->tables[numa_node];
	int tid = get_tid();

	hs_update(&table->hs, tid, key, addr);
}

void mptable_split(struct numa_table* old_table, struct pnode* new_pnode) {	
	int i, j, node;
	struct numa_table* new_table;
	struct mptable *old_m, *new_m;
	struct index_layer* i_layer = INDEX(bonsai);

	/* 1. allocate a new mapping table */
	new_table = numa_mptable_alloc(LOG(bonsai));
	new_pnode->table = new_table;
	new_table->pnode = new_pnode;
	new_table->forward = old_table;

	/* 2. update the index layer */
	i_layer->insert(i_layer->index_struct, pnode_anchor_key(new_pnode), new_table);

	/* 3. copy entries from @old_table to @new_table */
	for (node = 0; node < NUM_SOCKET; node ++) {
		old_m = MPTABLE_NODE(old_table, node);
		new_m = MPTABLE_NODE(new_table, node);

		hs_scan_and_split(&old_m->hs, &new_m->hs, pnode_max_key(new_pnode), new_pnode);
	}

	new_table->forward = NULL;
}

static void merge_one_log(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4) {
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
static int __mptable_scan(struct numa_table* table, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr_key) {
	struct hbucket* merge_buckets;
	struct hash_set* hs;
	struct mptable *m;
	struct hlist_node *hnode, *n_hnode;
	int node, i, j;
	struct oplog *l;
	scan_merge_ent* e;
	pentry_t** arr;
	pkey_t max_key = 0;

	merge_buckets = malloc(sizeof(struct hbucket) * NUM_MERGE_HASH_BUCKET);
	arr = malloc(sizeof(pentry_t*) * MAX_LEN);

	/* 1. scan mapping table, merge logs */
	for (node = 0; node < NUM_SOCKET; node ++) {
		m =  table->tables[node];
		hs_scan_and_ops(&m->hs, merge_one_log, merge_buckets, low, high, &max_key, NULL);
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

void numa_table_search_key(pkey_t key) {
	struct log_layer* layer = LOG(bonsai);
	struct numa_table* table;
	struct mptable* m;
	pkey_t find;
	int node, i = 0;
	
	list_for_each_entry(table, &layer->numa_table_list, list) {
		for (node = 0; node < NUM_SOCKET; node ++) {
			m = MPTABLE_NODE(table, node);
			//bonsai_print("numa table %016lx hs[%d]: %016lx\n", table, i++, &m->hs);
			hs_scan_and_ops(&m->hs, hs_search_key, key, NULL, NULL, NULL, NULL);
		}
	}

	bonsai_print("numa table find no such key[%lu]\n", key);
}

void dump_numa_table() {
	struct log_layer* layer = LOG(bonsai);
	struct numa_table* table;
	struct mptable* m;
	int node, i = 0;

	bonsai_print("====================================DUMP NUMA TABLE================================================\n");

	list_for_each_entry(table, &layer->numa_table_list, list) {
		for (node = 0; node < NUM_SOCKET; node ++) {
			m = MPTABLE_NODE(table, node);
			bonsai_print("Node[%d] table %016lx\n", node, m);
			hs_scan_and_ops(&m->hs, hs_print_entry, NULL, NULL, NULL, NULL, NULL);
			bonsai_print("\n");
		}
	}
}
