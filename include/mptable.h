#ifndef _MTABLE_H
#define _MTABLE_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <assert.h>

#include "common.h"
#include "spinlock.h"
#include "numa_util.h"
#include "hash_set.h"

struct mptable {
    struct hash_set hs;
    struct mptable* tables[NUM_SOCKET];
    struct pnode* pnode;
    struct list_head list;
    struct list_head numa_list;
};

#define mptable_ent_num(table)\
    htable_ent_num(table->htable)

extern struct mptable* mptable_alloc(struct pnode* pnode, int num);
extern int mptable_insert(struct mptable* mptable, pkey_t key, pval_t value);
extern int mptable_remove(struct mptable* mptable, pkey_t key);
extern int mptable_lookup(struct mptable* mptable, pkey_t key, pval_t result);
extern int mptable_update(struct mptable* mptable, pkey_t key, pval_t val);
extern void mptable_split(struct mptable* mptable, struct pnode* pnode);

#endif
