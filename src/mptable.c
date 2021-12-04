/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include "mptable.h"
#include "oplog.h"

static struct mptable* init_one_mptable() {
    struct mptable* mptable;

	mptable = (struct mptable*) numa_alloc_onnode(sizeof(struct mptable), node);
	hs_init(&mptable->hs);

    return mptable;
}

static void free_one_mptable(struct mptable *table) {
	numa_free(table, sizeof(struct mptabl));
}

struct numa_table* numa_mptable_alloc(struct list_head* head) {
	struct numa_table* tables;
	int node;

	tables = malloc(sizeof(struct numa_table));
	for (node = 0; node < NUM_SOCKET; node ++)
		tables->tables[node] = init_one_mptable();

	tables->pnode = NULL;
	list_add_tail(&tables->list, head);

	return tables;
}

void numa_mptable_free(struct numa_table* tables) {
	int node;

	for (node = 0; node < NUM_SOCKET; node ++)
		free_one_mptable(tables);

	free(tables);
}

int mptable_insert(struct numa_table* tables, int numa_node, pkey_t key, pval_t value) {
	struct mptable* mptable = MPTABLE_NODE(tables, numa_node);
	struct oplog* log;
	int tid = get_tid();

	log = oplog_insert(key, val, OP_INSERT, numa_node, mptable, maptable->pnode);
	hs_insert(mptable, tid, key, &log->o_kv.value);

	return 0;
}

int mptable_update(struct numa_table* tables, int num_node pkey_t key, pval_t* address) {
	struct mptable* mptable = MPTABLE_NODE(tables, numa_node);
	struct oplog* log;
	int tid = get_tid();

	log = oplog_insert(key, val, OP_INSERT, addr, maptable->pnode);
	hs_insert(mptable, tid, key, &log->o_kv.value);

	return 0;
}

int mptable_remove(struct mptable* mptable, pkey_t key) {
	pval_t* addr;
	int tid = get_tid();

	log = oplog_insert(key, 0, OP_REMOVE, addr, maptable->pnode);
	hs_insert(mptable, tid, key, &log->o_kv.v);

	return 0;
}

pval_t mptable_lookup(struct numa_table* mptables, pkey_t key) {
	struct oplog *log, *max_insert_log = NULL;
	int node, tid = get_tid();
	struct mptable *table;
	pval_t *addr;
	unsigned long max_insert_t = 0, max_remove_t = 0;
#ifndef BONSAI_SUPPORT_UPDATE
	int remove_cnt = 0;
#endif

	node = get_numa_node();
	table = MPTABLE_NODE(mptables, node);
	addr = hs_lookup(&table->hs, tid, key);
	if (!addr) {
		
	}
	
	for (node = 0; node < NUM_SOCKET; node ++) {
		table = MPTABLE_NODE(mptables, node);
		addr = hs_lookup(&table->hs, tid, key);

		if (addr_in_log(addr)) {
			/* case I: mapping table entry points to an oplog */
			log = OPLOG(addr);
			if (log->o_type == OP_REMOVE) {
#ifdef BONSAI_SUPPORT_UPDATE
				if (ordo_cmp_clock(log->o_stamp, max_remove_t)) 
					max_remove_t = log->o_stamp;
#else
				remove_cnt ++;
#endif
			}
#ifdef BONSAI_SUPPORT_UPDATE
			else {
				if (ordo_cmp_clock(log->o_stamp, max_insert_t)) {
					max_insert_t = log->o_stamp;
					max_insert_log = log;
				}
			}
#endif	
		} else if (addr_in_pnode(addr)) {
			/* case II: mapping table entry points to a pnode entry */
			
		} else {
			/* case III: mapping table entry is NULL */
			
		}
	}

#ifdef BONSAI_SUPPORT_UPDATE
	if (ordo_cmp_clock(max_remove_t, max_insert_t))
		return -ENOENT;
#else
	if (remove_cnt == NUM_SOCKET)
		return -ENOENT;
#endif

	if (max_insert_log)
		return max_insert_log->o_kv.v;
}

void mptable_update_addr(struct mptable *table, pkey_t key, pval_t* addr) {
	int tid = get_tid();

	hs_insert(&table->hs, tid, key, addr);
}

void mptable_split(struct numa_table* src, struct pnode* pnode) {	
	int tid, i, node, N = pnode->slot[0];
	struct numa_table* tables;
	struct mptable *table;
	pkey_t key;
	pval_t *addr;
	
	tables = numa_mptable_alloc(&LOG(bonsai)->mptable_list);

	for (i = 0; i < N; i ++) {
		key = pnode->e[pnode->slot[i]];
		for (node = 0; node < NUM_SOCKET; node ++) {
			table = MPTABLE_NODE(src->tables, node);
			if (addr = hs_lookup(&table->hs, tid, key)) {
				if (point_to_nvm(addr)) {
					hs_remove(&table->hs, tid, key);
					hs_insert(&tables->tables[node]->hs, tid, key, *addr);
				}
			}		
		}
	}

	INDEX(bonsai)->insert(pnode->e[pnode->slot[0]], tables);
}
