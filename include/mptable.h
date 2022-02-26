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

    struct pnode* pnode;
	struct mptable *forward;

    /*
     * The anchor key of last pnode.
     * Invariant: Each key in this mptable must be > lowerbound.
     */
    pkey_t lowerbound;

	struct list_head list;
};

extern int addr_in_log(unsigned long addr);
extern int addr_in_pnode(unsigned long addr);

struct log_layer;
extern struct mptable* mptable_alloc(struct log_layer* layer);
extern void mptable_free(struct log_layer* layer, struct mptable* table);

extern int mptable_insert(int numa_node, int cpu, pkey_t key, uint16_t k_len, pval_t value);
extern int mptable_update(int numa_node, int cpu, pkey_t key, uint16_t k_len, pval_t *address);
extern int mptable_remove(int numa_node, int cpu, pkey_t key, uint16_t k_len);
extern int mptable_lookup(int numa_node, pkey_t key, uint16_t k_len, int cpu, pval_t *val);
extern int mptable_scan(struct mptable* table, pkey_t low, uint16_t lo_len, pkey_t high, uint16_t hi_len, pval_t* val_arr);

extern void mptable_update_addr(struct mptable* table, pkey_t key, pval_t* addr);

extern void mptable_split(struct mptable* old_table, struct pnode* new_pnode, struct pnode* mid_pnode, pkey_t avg_key);

extern pval_t* __mptable_lookup(struct mptable* mptable, pkey_t key, int cpu);

extern void mptable_search_key(pkey_t key);
extern void check_mptable();
extern void dump_mptable();
extern void stat_mptable();

#ifdef __cplusplus
}
#endif

#endif
