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

#ifndef PNODE_FP
#define NUM_ENT_PER_PNODE      		32
#define NUM_BUCKET      			8
#define NUM_ENT_PER_BUCKET     		4
#define PNODE_BITMAP_FULL			(~(0UL))

enum {
	PNODE_DATA_CLEAN = 0,
	PNODE_DATA_STALE,
};

/*
 * persistent node definition in data layer
 */
struct pnode {
	/* 1st cache line */
	__le8				stale; /* indicate wether this pnode data is stale */
	char				padding1[7];
    __le64 				bitmap;
    rwlock_t* 			slot_lock;
	pkey_t				anchor_key;
	struct list_head 	list;

	/* 2rd - 10th cache line */
    pentry_t 			e[NUM_ENT_PER_PNODE];

	/* 11th cache line */
    __le8 				slot[NUM_ENT_PER_PNODE + 1]; /* slot[0]: how many entries */
	char				padding2[23];
	struct mptable* 	table;

	/* 12th cache line */
	rwlock_t* 			bucket_lock[NUM_BUCKET];

	/* 13th cache line */	
	__le64 				forward[NUM_SOCKET][NUM_BUCKET];	
}__packed;

#define PNODE_ANCHOR_KEY(pnode) 	pnode->anchor_key
#define PNODE_MAX_KEY(pnode) 		pnode->e[pnode->slot[pnode->slot[0]]].k
#define PNODE_MIN_KEY(pnode)		pnode->e[pnode->slot[1]].k

#define PNODE_SORTED_KEY(pnode, n) 	pnode->e[pnode->slot[n]].k
#define PNODE_SORTED_VAL(pnode, n) 	pnode->e[pnode->slot[n]].v

#define PNODE_BITMAP(node) 			((node)->bitmap)

static int key_cmp(pkey_t a, pkey_t b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

extern void sentinel_node_init();

extern int pnode_insert(pkey_t key, pval_t value, unsigned long time_stamp, int numa_node);
extern int pnode_remove(pkey_t key);
extern pval_t* pnode_lookup(struct pnode* pnode, pkey_t key);

extern pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node);

extern int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr);

extern void check_pnode(pkey_t key, struct pnode* pnode);
extern void print_pnode(struct pnode* pnode);
extern void print_pnode_summary(struct pnode* pnode);
extern void dump_pnode_list();
extern void dump_pnode_list_summary();
extern struct pnode* data_layer_search_key(pkey_t key);
#else

enum {
	PNODE_DATA_CLEAN = 0,
	PNODE_DATA_STALE,
};

#define NUM_ENTRY_PER_PNODE		47
#define PNODE_CACHE_SIZE		4

#define PNODE_BITMAP_FULL		0x7fffffffffff
#define CACHE_MASK			   0x3800000000000
/**/
struct pnode_meta {
	__le64		bitmap;
	rwlock_t 	lock;
	__le8		nru;
	__le8		fgprt[NUM_ENTRY_PER_PNODE];
};

struct pnode {
	/*cacheline 0*/
	struct pnode_meta 	meta;

	union atomic_u128_2	cache_e[PNODE_CACHE_SIZE];
	
	/*cacheline 1 ~ 4*/
	pentry_t 			e[NUM_ENTRY_PER_PNODE];
	
	pkey_t 				anchor_key;
	struct list_head 	list;
	struct mptable*		table;
	__le8				slot[NUM_ENTRY_PER_PNODE + 1];

	__le64 				forward[NUM_SOCKET];
	__le8				stale;
};

#define PNODE_LOCK(node)						((node)->meta.lock)
#define PNODE_FGPRT(node, i) 					((node)->meta.fgprt[i])
#define PNODE_NRU(node)							((node)->meta.nru)
#define PNODE_BITMAP(node) 						((node)->meta.bitmap)
#define PNODE_CACHE_ENT(node, i)				((union atomic_u128_2)AtomicLoad128_2(&(node)->cache_e[i]))
#define PNODE_SET_CACHE_ENT(node, i, old, new)	(AtomicCAS128_2(&(node)->cache_e[i], old, new))
#define PNODE_KEY(node, i)						((node)->e[i].k)
#define PNODE_VAL(node, i)						((node)->e[i].v)
#define PNODE_SORTED_KEY(node, i)				(PNODE_KEY(node, (node)->slot[i]))
#define PNODE_SORTED_VAL(node, i)				(PNODE_VAL(node, (node)->slot[i]))
#define PNODE_ANCHOR_KEY(node)					((node)->anchor_key)
#define PNODE_MAX_KEY(node)						(PNODE_SORTED_KEY(node, (node)->slot[0]))
#define PNODE_MIN_KEY(node)						(PNODE_SORTED_KEY(node, 1))

static int key_cmp(pkey_t a, pkey_t b) {
	if (a < b) return -1;
	if (a > b) return 1;
	return 0;
}

extern void sentinel_node_init();

extern int pnode_insert(pkey_t key, pval_t val, unsigned long time_stamp, int numa_node);
extern int pnode_remove(pkey_t key);
extern pval_t* pnode_lookup(struct pnode* pnode, pkey_t key);


extern pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node);

extern int scan_one_pnode(struct pnode* pnode, int n, pkey_t low, pkey_t high, pval_t* result, pkey_t* curr);

extern void check_pnode(pkey_t key, struct pnode* pnode);
extern void print_pnode(struct pnode* pnode);
extern void print_pnode_summary(struct pnode* pnode);
extern void dump_pnode_list();
extern void dump_pnode_list_summary();
extern struct pnode* data_layer_search_key(pkey_t key);

#endif

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
