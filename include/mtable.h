#ifndef _MTABLE_H
#define _MTABLE_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdint.h>
#include <assert.h>

#include "common.h"
#include "spinlock.h"
#include "hash.h"
#include "numa_util.h"
#include "list.h"

struct mtable {
    struct htable htable;
    int numa;
    struct mtable* slave[NUM_SOCKET];
    struct pnode* pnode;
    struct list_head list;
    struct list_head numa_list;
#if 0
    uint8_t numa_id;
    uint8_t total_ent; 
    uint8_t used_ent;
	char padding[1];
	rwlock_t lock;
    int64_t seeds[3];
    struct mtable* slave[NUM_SOCKET];
    struct pnode* pnode;
	struct list_head list;
	struct list_head numa_list;
	mtable_ent_t e[0];
#endif
};

#define mtable_ent_num(table)\
    htable_ent_num(table->htable)

extern struct mtable* mtable_alloc(struct pnode* pnode, int num);
extern int mtable_insert(struct mtable* mtable, pkey_t key, pval_t value);
extern int mtable_remove(struct mtable* mtable, pkey_t key);
extern int mtable_lookup(struct mtable* mtable, pkey_t key, pval_t result);
extern int mtable_update(struct mtable* mtable, pkey_t key, pval_t val);
extern void mtable_split(struct mtable* mtable, struct pnode* pnode);

#endif
