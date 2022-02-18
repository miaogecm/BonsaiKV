#ifndef LINK_LIST_H
#define LINK_LIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

#include "common.h"
#include "hp.h"

typedef size_t markable_t;

#define MAX_NUM_THREADS		48

struct ll_node {
    pkey_t key;
	pval_t* val;
    int is_sentinel_key;
    markable_t next;
};

struct linked_list {
    struct ll_node ll_head;  /* sentinel node, head the linked list */
    struct hp_item* HP[MAX_NUM_THREADS]; /* every thread has a hp_item* in it */
    unsigned int count_of_hp; /* upper bound of the hps in the system */
};

#define    IS_TAGGED(v, tag)    ((v) &  tag) 
#define    STRIP_TAG(v, tag)    ((v) & ~tag)
#define    TAG_VALUE(v, tag)    ((v) |  tag)

#define    HAS_MARK(markable_t_value)          (IS_TAGGED((markable_t_value), 0x1) == 0x1)
#define    STRIP_MARK(markable_t_value)        (STRIP_TAG((markable_t_value), 0x1))
#define    MARK_NODE(markable_t_value)         (TAG_VALUE(markable_t_value, 0x1))
#define    GET_NODE(markable_t_value)          ((struct ll_node*)markable_t_value)

extern void ll_init(struct linked_list* ll);
extern void ll_destroy(struct linked_list* ll);

extern struct ll_node* ll_find(struct linked_list* ll, int tid, pkey_t key, struct ll_node** pred, struct ll_node** succ);
extern int ll_insert(struct linked_list* ll, int tid, pkey_t key, pval_t* val, int update);
extern int ll_remove(struct linked_list* ll, int tid, pkey_t key);
extern struct ll_node* ll_lookup(struct linked_list* ll, int tid, pkey_t key);
extern void ll_print(struct linked_list* ll);
extern int ll_insert_node(struct linked_list* ll, int tid, struct ll_node* node);
#ifdef __cplusplus
}
#endif

#endif
