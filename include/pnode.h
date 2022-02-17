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

#if 0
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

#define pnode_anchor_key(pnode) pnode->anchor_key
#define pnode_max_key(pnode) pnode->e[pnode->slot[pnode->slot[0]]].k
#define pnode_min_key(pnode) pnode->e[pnode->slot[1]].k

#define pnode_entry_n_key(pnode, n) pnode->e[pnode->slot[n]].k
#define pnode_entry_n_val(pnode, n) pnode->e[pnode->slot[n]].v

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

#define NUM_ENTRY_PER_PNODE		11
#define PNODE_CACHE_SIZE		3


#define PNODE_BITMAP_FULL		0x3fff
#define FGPRT_MASK				0x7f

union pnode_meta {
	__le64	word8B[2];
	struct {
		__le16			:1;
		__le16	alt		:1;
		__le16	nru  	:PNODE_CACHE_SIZE;
		__le16	bitmap	:NUM_ENTRY_PER_PNODE;
		__le8	fgprt[NUM_ENTRY_PER_PNODE];
	}s;
}

struct pnode {
	/*cacheline 0*/
	union pnode_meta 	meta;
	pentry_t			cache_e[PNODE_CACHE_SIZE];
	
	/*cacheline 1 ~ 4*/
	pentry_t 			e[NUM_ENTRY_PER_PNODE];
	
	pkey_t 				anchor_key;
	struct list_head 	list;
	rwlock_t* 			lock;
	struct mptable*		table;
	__le8				slot[NUM_ENTRY_PER_PNODE + 1];

	__le64 				forward[NUM_SOCKET];
}

#define PNODE_FGPRT(node, i) 		((node)->meta.s.fgprt[i])
#define PNODE_BITAMP(node) 			((node)->meta.s.bitmap)
#define PNDOE_ALT(node)				((node)->meta.s.alt)
#define PNODE_CACHE_KEY(node, i)	((node)->cache_e[i].key)
#define PNODE_CACHE_VAL(node, i)	((node)->cache_e[i].val)
#define PNODE_KEY(node, i)			((node)->e[i].key)
#define PNODE_VAL(node, i)			((node)->e[i].val)
#define PNODE_SORTED_KEY(node, i)	(PNODE_KEY(node, (node)->slot[i]))
#define PNODE_SORTED_VAL(node, i)	(PNODE_VAL(node, (node)->slot[i]))
#define PNODE_ANCHOR_KEY(node)		((node)->anchor_key)
#define PNODE_MAX_KEY(node)			(PNODE_SORTED_KEY(node, (node)->slot[0]))

static int key_cmp(pkey_t a, pkey_t b) {
	if (a < b) return -1;
	if (a > b) return 1;
	return 0;
}

extern void sentinel_node_init();

extern int pnode_insert(pkey_t key, pval_t val, unsigned long time_stamp, int numa_node);
extern int pnode_remove(pket_t key);
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
