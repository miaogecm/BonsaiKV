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

enum {
	PNODE_DATA_CLEAN = 0,
	PNODE_DATA_STALE,
};

#define NUM_ENTRY_PER_PNODE		40

#define PNODE_BITMAP_FULL		0xffffffffffff

struct oplog;

struct pnode_meta {
	__le64		bitmap;
	__le8		slot[NUM_ENTRY_PER_PNODE + 1];
    char        padding[15];
} __packed;

/*
 * integer key: 24B
 * string key:  48B
 */
struct pnode_entry {
    struct pentry ent;
    uint16_t      epoch;
    __le64        stamp;
} __packed;

struct pnode {
    /* cacheline 0 */
	struct pnode_meta   meta;
	
	/*
	 * integer key: cacheline 1 ~ 15
	 * string key:  cacheline 1 ~ 30
	 */
	struct pnode_entry  e[NUM_ENTRY_PER_PNODE];

    /* cacheline 16/31 */
    rwlock_t            *lock;
	pkey_t 				anchor_key;
    struct pnode        *next;
};

#define PNODE_LOCK(node)						((node)->lock)
#define PNODE_BITMAP(node) 						((node)->meta.bitmap)
#define PNODE_PENT(node, i)                     ((node)->e[i])
#define PNODE_SORTED_PENT(node, i)				(PNODE_PENT(node, (node)->meta.slot[i]))
#define PNODE_ANCHOR_KEY(node)					((node)->anchor_key)
#define PNODE_MAX_KEY(node)						(PNODE_SORTED_PENT(node, (node)->meta.slot[0]).ent.k)
#define PNODE_MIN_KEY(node)						(PNODE_SORTED_PENT(node, 1).ent.k)

extern void sentinel_node_init(int node);

extern int pnode_insert(pkey_t key, pval_t val, pentry_t *old_ent);
extern int pnode_remove(pkey_t key);

extern pval_t* pnode_numa_move(struct pnode* pnode, pkey_t key, int numa_node, void* addr);

extern void check_pnode(pkey_t key, struct pnode* pnode);
extern void print_pnode(struct pnode* pnode);
extern void print_pnode_summary(struct pnode* pnode);
extern void dump_pnode_list(void (*printer)(struct pnode *pnode));
extern struct pnode* data_layer_search_key(pkey_t key);

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
