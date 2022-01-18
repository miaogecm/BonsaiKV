#ifndef _MTABLE_H
#define _MTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include "numa_config.h"
#include "hash_set.h"
#include "list.h"

struct mptable {
	struct hash_set hs;
};

struct numa_table {
	struct mptable* tables[NUM_SOCKET];
	struct pnode* pnode;
	struct list_head list;
};

#define MPTABLE_NODE(TABLE, NODE)	TABLE->tables[NODE]

extern int addr_in_log(unsigned long addr);
extern int addr_in_pnode(unsigned long addr);

struct log_layer;
extern struct numa_table* numa_mptable_alloc(struct log_layer* layer);
extern void numa_mptable_free(struct numa_table* tables);

extern int mptable_insert(struct numa_table* tables, int numa_node, int cpu, pkey_t key, pval_t value);
extern int mptable_update(struct numa_table* tables, int num_node, int cpu, pkey_t key, pval_t* address);
extern int mptable_remove(struct numa_table* tables, int numa_node, int cpu, pkey_t key);
extern int mptable_lookup(struct numa_table* tables, pkey_t key, int cpu, pval_t* val);
extern int mptable_scan(struct numa_table* table, pkey_t high, pkey_t low, pval_t* val_arr);

extern void mptable_update_addr(struct numa_table* tables, int numa_node, pkey_t key, pval_t* addr);

extern void mptable_split(struct numa_table* src, struct pnode* pnode);

#ifdef __cplusplus
}
#endif

#endif
