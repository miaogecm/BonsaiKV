#ifndef BONSAI_RCU_H
#define BONSAI_RCU_H

#include <stdint.h>
#include "atomic.h"

#define RCU_MAX_THREAD_NUM      64
#define RCU_MAX_OP              100

typedef struct {
    /*
     * A set bit means that the corresponding thread has experienced
     * a quiescent state in this round.
     */
    atomic64_t qs_bmp;
    atomic64_t thr_bmp;
} fuzzy_barrier_t;

void fb_set_tid(int tid);
void fb_init(fuzzy_barrier_t *fb);
void fb_thread_online(fuzzy_barrier_t *fb);
void fb_thread_offline(fuzzy_barrier_t *fb);
int fb_try_barrier(fuzzy_barrier_t *fb);
void fb_end_barrier(fuzzy_barrier_t *fb);

typedef void (*rcu_cb_t)(void *);

struct rcu_cb_item {
    rcu_cb_t cb;
    void *aux;
    struct rcu_cb_item *next;
};

struct rcu_cb_queue {
    struct rcu_cb_item *next_head, *next_tail;
    struct rcu_cb_item *curr_head;
    struct rcu_cb_item *prev_head;
};

typedef struct {
    fuzzy_barrier_t fb;
    int fb_cnt;
    /* Thread-local callback queue */
    struct rcu_cb_queue cb[RCU_MAX_THREAD_NUM];
} rcu_t;

static inline void rcu_thread_online(rcu_t *rcu) {
    fb_thread_online(&rcu->fb);
}

static inline void rcu_thread_offline(rcu_t *rcu) {
    fb_thread_offline(&rcu->fb);
}

void rcu_init(rcu_t *rcu);
void call_rcu(rcu_t *rcu, rcu_cb_t cb, void *aux);
void rcu_quiescent(rcu_t *rcu);
int  rcu_now(rcu_t *rcu);
void rcu_synchronize(rcu_t *rcu, int since);

#endif //BONSAI_RCU_H
