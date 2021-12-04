#ifndef _MTABLE_H
#define _MTABLE_H

#include "common.h"
#include "numa_util.h"
#include "hash_set.h"

struct mptable {
	struct hash_set hs;
}

struct numa_table {
	struct mptable* tables[NUM_SOCKET];
	struct pnode* pnode;
	struct list_head* list;
};

#define MPTABLE_NODE(TABLE, NODE)	\
	(struct mptable*)TABLE->tables[NODE]

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
extern void numa_mptable_free(void* tables);

extern int mptable_insert(struct mptable* mptable, pkey_t key, pval_t value);
extern int mptable_update(struct mptable* mptable, pkey_t key, pval_t value);
extern int mptable_remove(struct mptable* mptable, pkey_t key);
extern pval_t mptable_lookup(struct numa_table* tables, pkey_t key);
extern void mptable_update_addr(struct numa_table* table, int numa_node, pkey_t key, pval_t* addr);

#endif
