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

typedef struct {
    int 		e_used;
    rwlock_t 	e_lock;
	pkey_t 		e_key;
    void* 		e_addr;
} mtable_ent_t;

struct mtable {
    uint32_t total_ent; 
    uint32_t used_ent;
	rwlock_t lock;
    int64_t seeds[3];
    struct pnode* pnode;
	struct list_head list;
	mtable_ent_t e[0];
};

extern int mtable_insert(struct mtable* table, pkey_t key, pval_t value);
extern int mtable_remove(struct mtable* table, pkey_t key);
extern pval_t mtable_lookup(struct mtable* table, pkey_t key);

#endif
