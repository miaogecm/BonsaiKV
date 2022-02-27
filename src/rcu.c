/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * A QSBR(Quiescent State Based Reclamation) RCU Implementation
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

#include <stdlib.h>
#include "rcu.h"
#include "arch.h"

#define THR_MASK(tid)       (1ul << (tid))

static __thread int my_tid;

void fb_set_tid(int tid) {
    my_tid = tid;
}

void fb_init(fuzzy_barrier_t *fb) {
    atomic64_set(&fb->qs_bmp, 0);
    atomic64_set(&fb->thr_bmp, 0);
}

void fb_thread_online(fuzzy_barrier_t *fb) {
    atomic64_or(THR_MASK(my_tid), &fb->thr_bmp);
    smp_mb();
}

void fb_thread_offline(fuzzy_barrier_t *fb) {
    unsigned long mask = THR_MASK(my_tid);
    atomic64_or(mask, &fb->qs_bmp);
    atomic64_and(~mask, &fb->thr_bmp);
}

int fb_done_quiescent(fuzzy_barrier_t *fb) {
    return (atomic64_read(&fb->qs_bmp) & THR_MASK(my_tid)) != 0;
}

int fb_try_barrier(fuzzy_barrier_t *fb) {
    unsigned long mask = THR_MASK(my_tid);
    unsigned long origin, expected;

    expected = atomic64_read(&fb->thr_bmp);

    origin = atomic64_fetch_or(mask, &fb->qs_bmp);
    if (origin & mask) {
        /* Already experienced a quiescent state in this round. */
        return 0;
    }

    if (((origin | mask) & expected) != expected) {
        /* There're still some always-non-quiescent threads. */
        return 0;
    }

    return 1;
}

int fb_end_barrier(fuzzy_barrier_t *fb) {
    atomic64_set(&fb->qs_bmp, 0);
}

void rcu_init(rcu_t *rcu) {
    int i;
    for (i = 0; i < RCU_MAX_THREAD_NUM; i++) {
        rcu->cb[i].next_head = rcu->cb[i].next_tail = NULL;
        rcu->cb[i].curr_head = NULL;
        rcu->cb[i].prev_head = NULL;
    }
}

void call_rcu(rcu_t *rcu, rcu_cb_t cb, void *aux) {
    /* RCU callback will always run at the local thread. */
    struct rcu_cb_queue *q = &rcu->cb[my_tid];
    struct rcu_cb_item *item = malloc(sizeof(*item));
    item->cb = cb;
    item->aux = aux;
    item->next = NULL;
    if (!q->next_head) {
        q->next_head = item;
    } else {
        q->next_tail->next = item;
    }
    q->next_tail = item;
}

void rcu_quiescent(rcu_t *rcu) {
    struct rcu_cb_queue *q = &rcu->cb[my_tid];
    fuzzy_barrier_t *fb = &rcu->fb;
    struct rcu_cb_item *item, *tmp;
    /*
     * Each thread's RCU callbacks will run at the first quiescent point
     * between two adjacent fuzzy barriers.
     */
    if (!fb_done_quiescent(fb)) {
        item = q->prev_head;
        while (item) {
            item->cb(item->aux);
            tmp = item->next;
            free(item);
            item = tmp;
        }
        q->prev_head = q->curr_head;
        q->curr_head = q->next_head;
        q->next_head = q->next_tail = NULL;
    }
    if (fb_try_barrier(fb)) {
        fb_end_barrier(fb);
    }
}
