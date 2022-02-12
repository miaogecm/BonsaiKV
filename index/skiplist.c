/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 
 * A skip list implementation.
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <sched.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>

#include "skiplist.h"

#include "bench.h"

int sl_cmp(pkey_t x, pkey_t y) {
    if (x > y) return 1;
    if (x == y) return 0;
    return -1;
}

static int random_levels(struct sl_kv* sl) {
    int levels = 1;
    
    while (levels <= sl->levels && (rand() & 1)) {
        levels++;
    }
    
    if (levels > MAX_LEVELS)
        levels = MAX_LEVELS;

    if (levels > sl->levels) {
        SYNC_ADD(&sl->levels, 1);
    }
    
    return levels;
}

static struct kv_node* alloc_node(int levels) {
    struct kv_node* node;
    int size = sizeof(struct kv_node) + levels * sizeof(struct kv_node*);
    node = (struct kv_node*)malloc(size);
    assert(node);

    node->marked = 0;
    node->fully_linked = 0;

    spin_lock_init(&node->lock);
    memset(node->next, 0, sizeof(struct kv_node*) * levels);
    node->levels = levels;
    
    return node;
}

static void free_node(struct kv_node* node) {
    // free(node);
}

void* sl_init() {
    struct sl_kv* sl = (struct sl_kv*)malloc(sizeof(struct sl_kv));
    int i;

#ifndef UNRAND
    srand(time(0));
#endif

    sl_head(sl) = alloc_node(MAX_LEVELS);
    sl_head(sl)->fully_linked = 1;
    sl_tail(sl) = alloc_node(MAX_LEVELS);
    sl_tail(sl)->fully_linked = 1;

    for (i = 0; i < MAX_LEVELS; i++)
        sl_head(sl)->next[i] = sl_tail(sl);
    sl->levels = 1;

    return (struct sl_kv*)sl;
}

void sl_destory(void* index_struct) {
    struct sl_kv* sl = (struct sl_kv*)index_struct;
    struct kv_node* curr, *prev;

    curr = sl_head(sl);
    while(curr != sl_tail(sl)) {
        prev = curr;
        curr = curr->next[0];
        free(prev);
    }
    free(sl_tail(sl));
    free(sl);
}

// lowerbound in fact
void* sl_lookup(void* index_struct, pkey_t key) {
    struct sl_kv* sl = (struct sl_kv*)index_struct;
    struct kv_node *pred, *curr;
    int i;
    
    pred = sl_head(sl);
    for (i = sl_level(sl); i >= 0; i--) {
        curr = pred->next[i];
        while(curr != sl_tail(sl) && sl_cmp(key, node_key(curr)) > 0) {
            pred = curr;
            curr = pred->next[i];
        }
    }

    return (void*)node_val(curr);
}

static int kv_find(void* index_struct, pkey_t key, struct kv_node** preds, struct kv_node** succs) {
    struct sl_kv* sl = (struct sl_kv*)index_struct;
    struct kv_node *pred, *curr;
    int i, found_l = -1;

    pred = sl_head(sl);
    for (i = sl_level(sl); i >= 0; i--) {
        curr = pred->next[i];
        while(curr != sl_tail(sl) && sl_cmp(key, node_key(curr)) > 0) {
            pred = curr;
            curr = pred->next[i];
        }
        if (!found_l && sl_cmp(key, node_key(curr)) == 0) {
            found_l = i;
        }
        preds[i] = pred;
        succs[i] = curr;
    }
    
    return found_l;
}

int sl_insert(void* index_struct, pkey_t key, void* val) {
    struct sl_kv* sl = (struct sl_kv*)index_struct;
    int levels;
    struct kv_node* preds[MAX_LEVELS];
    struct kv_node* succs[MAX_LEVELS];
    struct kv_node *pred, *curr, *succ, *new_node;
    int found_l, i, high_l, full_locked;

    levels = random_levels(sl);
    while(1) {
        found_l = kv_find(sl, key, preds, succs);
        if (found_l != -1) {
            curr = succs[found_l];
            if (!curr->marked) {
                while(!curr->fully_linked);
                return -EEXIST;
            }
            continue;
        }

        full_locked = 1;
        for (i = 0; i < levels; i++) {
            pred = preds[i];
            succ = succs[i];
            if (i == 0 || pred != preds[i - 1]) {
                spin_lock(&pred->lock);
            }
            high_l = i;
            if (pred->marked || succ->marked || pred->next[i] != succ) {
                full_locked = 0;
                break;
            }
        }
        if (!full_locked) {
            for (i = 0; i <= high_l; i++) {
                pred = preds[i];
                if (i == 0 || pred != preds[i - 1]) {
                    spin_unlock(&pred->lock);
                }
            }
            continue;
        }

        new_node = alloc_node(levels);
        node_key(new_node) = key;
        node_val(new_node) = (pval_t)val;

        for (i = 0; i < levels; i++) {
            new_node->next[i] = succs[i];
            preds[i]->next[i] = new_node;
        }
        new_node->fully_linked = 1;

        for (i = 0; i < levels; i++) {
            pred = preds[i];
            if (i == 0 || pred != preds[i - 1]) {
                spin_unlock(&pred->lock);
            }
        }

        return 0;
    }
}

int kv_update(void* index_struct, pkey_t key, void* val) {
    struct sl_kv* sl = (struct sl_kv*)index_struct;
    int levels;
    struct kv_node* preds[MAX_LEVELS];
    struct kv_node* succs[MAX_LEVELS];
    struct kv_node *pred, *curr, *succ, *new_node;
    int found_l, i, high_l, full_locked;

    levels = random_levels(sl);
    while(1) {
        found_l = kv_find(sl, key, preds, succs);
        if (found_l != -1) {
            curr = succs[found_l];
            if (!curr->marked) {
                while(!curr->fully_linked);
                node_val(curr) = (pval_t)val;
                return 0;
            }
            continue;
        }

        full_locked = 1;
        for (i = 0; i < levels; i++) {
            pred = preds[i];
            succ = succs[i];
            if (i == 0 || pred != preds[i - 1]) {
                spin_lock(&pred->lock);
            }
            high_l = i;
            if (pred->marked || succ->marked || pred->next[i] != succ) {
                full_locked = 0;
                break;
            }
        }
        if (!full_locked) {
            for (i = 0; i <= high_l; i++) {
                pred = preds[i];
                if (i == 0 || pred != preds[i - 1]) {
                    spin_unlock(&pred->lock);
                }
            }
            continue;
        }

        new_node = alloc_node(levels);
        node_key(new_node) = key;
        node_val(new_node) = (pval_t)val;

        for (i = 0; i < levels; i++) {
            new_node->next[i] = succs[i];
            preds[i]->next[i] = new_node;
        }
        new_node->fully_linked = 1;

        for (i = 0; i < levels; i++) {
            pred = preds[i];
            if (i == 0 || pred != preds[i - 1]) {
                spin_unlock(&pred->lock);
            }
        }

        return 0;
    }
}

int sl_remove(void* index_struct, pkey_t key) {
    struct sl_kv* sl = (struct sl_kv*)index_struct;
    struct kv_node* preds[MAX_LEVELS];
    struct kv_node* succs[MAX_LEVELS];
    struct kv_node *victim, *pred;
    int found_l, i, is_marked, high_l, full_locked;

    is_marked = 0;
    while(1) {
        found_l = kv_find(sl, key, preds, succs);
        if (found_l != -1) {
            victim = succs[found_l];
        }
        if (is_marked || 
        (found_l != -1 && victim->fully_linked && victim->marked)) {
            if (!is_marked) {
                assert(found_l == victim->levels - 1);
                spin_lock(&victim->lock);
                if (victim->marked) {
                    spin_unlock(&victim->lock);
                    return -ENONET;
                }
                victim->marked = 1;
                is_marked = 1;
            }

            full_locked = 1;
            for (i = 0; i <= found_l; i++) {
                pred = preds[i];
                if (i == 0 || pred != preds[i]) {
                    spin_lock(&pred->lock);
                }
                high_l = i;
                if (pred->marked || pred->next[i] != victim) {
                    full_locked = 0;
                    break;
                }
            }
            if (!full_locked) {
                for (i = 0; i <= high_l; i++) {
                    if (i == 0 || pred != preds[i - 1]) {
                        spin_unlock(&pred->lock);
                    }
                }
                continue;
            }
            for (i = 0; i <= found_l; i++) {
                preds[i]->next[i] = victim->next[i];
            }
            spin_unlock(&victim->lock);
            free_node(victim);

            return 0;
        } else {
            return -ENOENT;
        }
    }
}

int sl_scan(void* index_struct, pkey_t min, pkey_t max) {
    //struct sl_kv* sl = (struct sl_kv*)index_struct;
    return 0;
}

// single-thread ONLY
void kv_print(void* index_struct) {
	struct sl_kv *sl = (struct sl_kv*)index_struct;
    struct kv_node* node = sl_head(sl)->next[0];

	printf("index layer:\n");
    while(node != sl_tail(sl)) {
        printf("<%lu, %016lx> -> ", node->kv.k, node->kv.v);
        node = node->next[0];
    }

	printf("NULL\n");
}

int main() {
	return bench("skiplist", sl_init, sl_destory, sl_insert, sl_remove, sl_lookup, sl_scan);
}
