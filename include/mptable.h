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

#define ENT_REMOVE	0x1

static inline int point_to_nvm(void* addr) {
	return 0;
}

static inline int addr_in_log(void* addr) {
	return 0;
}

static inline int addr_in_pnode(void* addr) {
	return 0;
}

extern struct numa_table* numa_mptable_alloc(struct list_head* head);
extern void numa_mptable_free(struct numa_table* tables);

extern int mptable_insert(struct numa_table* tables, int numa_node, pkey_t key, pval_t value, int cpu);
extern int mptable_update(struct numa_table* tables, int num_node, pkey_t key, pval_t* address);
extern int mptable_remove(struct numa_table* tables, int numa_node, pkey_t key int cpu);
extern pval_t mptable_lookup(struct numa_table* tables, pkey_t key int cpu);

extern void mptable_update_addr(struct numa_table* tables, int numa_node, pkey_t key, pval_t* addr);

extern void mptable_split(struct numa_table* src, struct pnode* pnode);

#ifdef __cplusplus
}
#endif

#endif
