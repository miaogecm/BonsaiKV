#ifndef __PNODE_H
#define __PNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint-gcc.h>
#include <stdlib.h>

#include "arch.h"
#include "rwlock.h"
#include "list.h"
#include "common.h"
#include "region.h"
#include "hwconfig.h"
#include "log_layer.h"

typedef uint32_t pnoid_t;

#define PNOID_NULL              (-1u)

struct data_layer {
	struct data_region region[NUM_DIMM];

    pnoid_t free_list;

    pnoid_t tofree_head, tofree_tail;

    pnoid_t sentinel;

    /* Protect the pnode list. */
	spinlock_t plist_lock;

	unsigned epoch;
    unsigned *epoch_table[NUM_SOCKET];
};

typedef struct {
    enum {
        PBO_NONE = 0,
        PBO_INSERT,
        PBO_REMOVE
    }      type;
    int    done;
    pkey_t key;
    pval_t val;
} pbatch_op_t;

typedef struct {
    pbatch_op_t *start;
    size_t len;
    struct list_head list;
} pbatch_node_t;

typedef struct {
    struct list_head *list, *curr;
    size_t i;
} pbatch_cursor_t;

static inline void pbatch_cursor_init(pbatch_cursor_t *cursor, struct list_head *list) {
    cursor->list = list;
    cursor->curr = list->next;
    cursor->i = 0;
}

static inline int pbatch_cursor_is_end(pbatch_cursor_t *cursor) {
    return cursor->curr == cursor->list;
}

static inline void pbatch_cursor_forward(pbatch_cursor_t *cursor, size_t stride) {
    pbatch_node_t *node;
    size_t avail;
    while (stride) {
        assert(!pbatch_cursor_is_end(cursor));
        node = list_entry(cursor->curr, pbatch_node_t, list);
        avail = node->len - cursor->i;
        if (stride < avail) {
            cursor->i += stride;
            break;
        } else {
            cursor->curr = cursor->curr->next;
            cursor->i = 0;
            stride -= avail;
        }
    }
}

static inline void pbatch_cursor_inc(pbatch_cursor_t *cursor) {
    pbatch_cursor_forward(cursor, 1);
}

static inline pbatch_op_t *pbatch_cursor_get(pbatch_cursor_t *cursor) {
    pbatch_node_t *node = list_entry(cursor->curr, pbatch_node_t, list);
    assert(!pbatch_cursor_is_end(cursor));
    return &node->start[cursor->i];
}

static inline void pbatch_list_create(struct list_head *list, pbatch_op_t *start, size_t len) {
    pbatch_node_t *node;
    assert(len);
    node = malloc(sizeof(*node));
    node->start = start;
    node->len = len;
    INIT_LIST_HEAD(&node->list);
    list_add_tail(&node->list, list);
}

static inline void pbatch_list_destroy(struct list_head *list) {
    pbatch_node_t *node, *tmp;
    list_for_each_entry_safe(node, tmp, list, list) {
        free(node);
    }
}

static void pbatch_list_dump(struct list_head *list) {
    pbatch_node_t *node;

    printf("----------pbatch list dump start----------\n");
    list_for_each_entry(node, list, list) {
        printf("list: %016lx, next: %016lx, size: %d, start: %016lx\n", &(node->list), node->list.next, node->len, node->start);
    }
    printf("----------pbatch list dump end----------\n");
}

static inline size_t pbatch_list_len(struct list_head *list) {
    pbatch_node_t *node;
    size_t len = 0;
    list_for_each_entry(node, list, list) {
        len += node->len;
    }
    return len;
}

static inline void pbatch_list_split(struct list_head *dst, pbatch_cursor_t *cursor) {
    pbatch_node_t *node = list_entry(cursor->curr, pbatch_node_t, list), *next;
    assert(!pbatch_cursor_is_end(cursor));
    if (cursor->i) {
        /* Split the node itself. */
        next = malloc(sizeof(*next));
        next->start = node->start + cursor->i;
        next->len = node->len - cursor->i;
        node->len = cursor->i;
        INIT_LIST_HEAD(&next->list);
        list_add(&next->list, &node->list);
        node = next;
    }
    list_replace_init(cursor->list, dst);
    list_cut_position(cursor->list, dst, node->list.prev);
    pbatch_cursor_init(cursor, dst);
}

static inline void pbatch_list_merge(struct list_head *dst, struct list_head *src) {
    list_splice(src, dst);
}

static inline pbatch_op_t *pbatch_list_get(struct list_head *list, size_t i) {
    pbatch_cursor_t cursor;
    pbatch_cursor_init(&cursor, list);
    pbatch_cursor_forward(&cursor, i);
    return pbatch_cursor_get(&cursor);
}

int pnode_numa_node(uint32_t pno);

static inline int pnode_color(pnoid_t pno) {
    return pnode_numa_node(pno);
}

pnoid_t pnode_sentinel_init();

void pnode_split_and_recolor(pnoid_t *pnode, pnoid_t *sibling, pkey_t *cut, int lc, int rc);
void pnode_run_batch(log_state_t *lst, pnoid_t pnode, struct list_head *pbatch_list);

pkey_t pnode_get_lfence(pnoid_t pnode);
void pnode_prefetch_meta(pnoid_t pnode);
int pnode_lookup(pnoid_t pnode, pkey_t key, pval_t *val);
int is_in_pnode(pnoid_t pnode, pkey_t key);

void pnode_recycle();

static inline void pnode_split(pnoid_t *pnode, pnoid_t *sibling, pkey_t *cut) {
    pnode_split_and_recolor(pnode, sibling, cut, pnode_color(*pnode), pnode_color(*sibling));
}

static inline void pnode_recolor(pnoid_t *pnode, int c) {
    pnode_split_and_recolor(pnode, NULL, NULL, c, c);
}

void begin_invalidate_unref_entries(unsigned *since);
void end_invalidate_unref_entries(const unsigned *since);

struct data_layer;

int data_layer_init(struct data_layer *layer);
void data_layer_deinit(struct data_layer* layer);

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
