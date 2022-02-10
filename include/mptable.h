#ifndef _MTABLE_H
#define _MTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include "numa_config.h"
#include "hash_set.h"
#include "list.h"
#include "rwlock.h"

struct mptable {
	struct hash_set hs;
	rwlock_t rwlock;
};

struct numa_table {
	struct mptable* tables[NUM_SOCKET];
	struct pnode* pnode;
	struct numa_table* forward;
	struct list_head list;
    /*
     * The anchor key of last pnode.
     * Invariant: Each key in this numa table must be > lowerbound.
     */
    pkey_t lowerbound;
};

#define MPTABLE_NODE(TABLE, NODE)	TABLE->tables[NODE]

extern int addr_in_log(unsigned long addr);
extern int addr_in_pnode(unsigned long addr);

struct log_layer;
extern struct numa_table* numa_mptable_alloc(struct log_layer* layer);
extern void numa_mptable_free(struct numa_table* tables);

extern int mptable_insert(int numa_node, int cpu, pkey_t key, pval_t value);
extern int mptable_update(int numa_node, int cpu, pkey_t key, pval_t *address);
extern int mptable_remove(int numa_node, int cpu, pkey_t key);
extern int mptable_lookup(int numa_node, pkey_t key, int cpu, pval_t *val);
extern int mptable_scan(struct numa_table* table, pkey_t high, pkey_t low, pval_t* val_arr);

extern void mptable_update_addr(struct numa_table* tables, int numa_node, pkey_t key, pval_t* addr);

extern void mptable_split(struct numa_table* old_table, struct pnode* new_pnode, struct pnode* mid_pnode, pkey_t avg_key);

extern pval_t* __mptable_lookup(struct numa_table* mptables, pkey_t key, int cpu);

extern void numa_table_search_key(pkey_t key);
extern void check_numa_table();
extern void dump_numa_table();
extern void stat_numa_table();

#ifdef __cplusplus
}
#endif

#endif
