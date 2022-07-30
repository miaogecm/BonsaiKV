#include <stdio.h>
#include <stdint.h>
#include <numa.h>

#include "mcs4.h"
#include "arch.h"
#include "atomic.h"

struct qnode {
    mcs4_t *lock;
    struct qnode *next;
    int spin;
} ____cacheline_aligned2;

#define NULL_QNODE          (-1)

#define MAX_NR_THREADS      128
#define MAX_HOLDING_LOCKS   2

static atomic_t tids = ATOMIC_INIT(0);
static struct qnode *qtable[MAX_NR_THREADS];

static __thread int tid = -1;

static inline uint16_t find_slot(mcs4_t *lock) {
    int i;
    for (i = 0; i < MAX_HOLDING_LOCKS; i++) {
        if (qtable[tid][i].lock == lock) {
            return i;
        }
    }
    assert(0);
}

void mcs4_register_thread() {
    tid = atomic_add_return(1, &tids) - 1;
    qtable[tid] = numa_alloc_local(MAX_HOLDING_LOCKS * sizeof(struct qnode));
}

static inline void auto_register_thread() {
    if (unlikely(tid == -1)) {
        mcs4_register_thread();
    }
}

void mcs4_init(mcs4_t *lock) {
    lock->val = NULL_QNODE;
}

void mcs4_lock(mcs4_t *lock) {
    mcs4_t pred_qid, my_qid;
    struct qnode *qnode;
    uint16_t slot;

    auto_register_thread();

    slot = find_slot(NULL);
    qnode = &qtable[tid][slot];
    my_qid.tid = tid;
    my_qid.slot = slot;

    /* put my qnode to the tail of queue */
    qnode->next = NULL;
    do {
        pred_qid.val = ACCESS_ONCE(lock->val);
    } while (!cmpxchg2(&lock->val, pred_qid.val, my_qid.val));

    /* empty queue, take the lock now */
    if (pred_qid.val == NULL_QNODE) {
        goto locked;
    }

    /* set spin flag */
    qnode->spin = 1;
    barrier();

    /* link to predecessor */
    qtable[pred_qid.tid][pred_qid.slot].next = qnode;

    /* wait for the lock */
    while (ACCESS_ONCE(qnode->spin)) {
        cpu_relax();
    }

locked:
    qnode->lock = lock;
}

void mcs4_unlock(mcs4_t *lock) {
    struct qnode *qnode;
    uint16_t slot;
    mcs4_t my_qid;

    slot = find_slot(lock);
    qnode = &qtable[tid][slot];
    my_qid.tid = tid;
    my_qid.slot = slot;

    if (!qnode->next) {
        /* assume that I am the tail */
        if (cmpxchg2(&lock->val, my_qid.val, NULL_QNODE)) {
            /* I am indeed the tail. */
            goto unlocked;
        }

        /* The real tail will set my @next. */
        while (!ACCESS_ONCE(qnode->next)) {
            cpu_relax();
        }
    }

    /* pass lock to the successor */
    qnode->next->spin = 0;

unlocked:
    qnode->lock = NULL;
}
