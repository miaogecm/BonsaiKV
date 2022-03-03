#ifndef __PNODE_H
#define __PNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "arch.h"
#include "rwlock.h"
#include "list.h"
#include "common.h"
#include "numa_config.h"
#include "atomic128_2.h"
#include "nab.h"

enum {
	PNODE_DATA_CLEAN = 0,
	PNODE_DATA_STALE,
};

#define NUM_ENTRY_PER_PNODE		48

#define PNODE_BITMAP_FULL		0xffffffffffff

struct oplog;

/*128 bytes*/
struct pnode_meta {
    /*cacheline 0*/
	__le64		bitmap;
	__le8		fgprt[NUM_ENTRY_PER_PNODE];
    char        padding[8];

    /*cacheline 1 (read only)*/
    rwlock_t    *lock;
    char        padding0[CACHELINE_SIZE - 8];
};

struct pnode {
    char                cls[0][CACHELINE_SIZE];

	/*cacheline 0 ~ 1*/
	struct pnode_meta 	meta;
	
	/*cacheline 2 ~ 13*/
	pentry_t 			e[NUM_ENTRY_PER_PNODE];
	
	/*cacheline 14*/
	__le8				slot[NUM_ENTRY_PER_PNODE + 1];
	__le8				stale;
	__le8				padding1[CACHELINE_SIZE - NUM_ENTRY_PER_PNODE - 2];

    char                nab_last[0];

	/*cacheline 15 (always in node 0)*/
	pkey_t 				anchor_key;
    struct pnode __node(0) *next;
};

#define PNODE_LOCK(node)						((node)->meta.lock)
#define PNODE_FGPRT(node, i) 					((node)->meta.fgprt[i])
#define PNODE_BITMAP(node) 						((node)->meta.bitmap)
#define PNODE_KEY(node, i)						((node)->e[i].k)
#define PNODE_VAL(node, i)						((node)->e[i].v)
#define PNODE_SORTED_KEY(node, i)				(PNODE_KEY(node, (node)->slot[i]))
#define PNODE_SORTED_VAL(node, i)				(PNODE_VAL(node, (node)->slot[i]))
#define PNODE_ANCHOR_KEY(node)					((node)->anchor_key)
#define PNODE_MAX_KEY(node)						(PNODE_SORTED_KEY(node, (node)->slot[0]))
#define PNODE_MIN_KEY(node)						(PNODE_SORTED_KEY(node, 1))

extern void sentinel_node_init();

extern int pnode_insert(pkey_t key, pval_t *val);
extern int pnode_remove(pkey_t key);

extern pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node, void* addr);

extern int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr);

extern void check_pnode(pkey_t key, struct pnode* pnode);
extern void print_pnode(struct pnode* pnode);
extern void print_pnode_summary(struct pnode* pnode);
extern void dump_pnode_list(void (*printer)(struct pnode __node(my) *pnode));
extern struct pnode* data_layer_search_key(pkey_t key);

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
