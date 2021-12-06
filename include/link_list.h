#ifndef LINK_LIST_H
#define LINK_LIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

#include "common.h"
#include "numa_config.h"

typedef size_t markable_t;

struct ll_node {
    pkey_t key;
	pval_t val;
    markable_t next;
};

struct linked_list {
    struct ll_node ll_head;  /* sentinel node, head the linked list */
    struct hp_item* HP[NUM_CPU]; /* every thread has a hp_item* in it */
    unsigned int count_of_hp; /* upper bound of the hps in the system */
};

extern void ll_init(struct linked_list* ll);
extern void ll_destroy(struct linked_list* ll);

extern struct ll_node* ll_find(struct linked_list* ll, int tid, pkey_t key, struct ll_node** pred, struct ll_node** succ);
extern int ll_insert(struct linked_list* ll, int tid, pkey_t key, pval_t val);
extern int ll_remove(struct linked_list* ll, int tid, pkey_t key);
extern int ll_lookup(struct linked_list* ll, int tid, pkey_t key);
extern void ll_print(struct linked_list* ll);

#ifdef __cplusplus
}
#endif

#endif
