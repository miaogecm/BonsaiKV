/*
 * BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System
 *
 * Statistics counters
 */

#define _GNU_SOURCE
#include "bonsai.h"
#include "counter.h"
#include "config.h"

#define NR_THREAD_MAX      (NUM_CPU * 2)

struct counter counters[NR_THREAD_MAX];
atomic_t nr_thread_counter;
static __thread int my_tid = -1;

static void counter_init(struct counter *counter) {
    memset(counter, 0, sizeof(*counter));
}

struct counter *this_counter() {
    if (unlikely(my_tid == -1)) {
        my_tid = atomic_add_return(1, &nr_thread_counter) - 1;
        counter_init(&counters[my_tid]);
    }
    return &counters[my_tid];
}
