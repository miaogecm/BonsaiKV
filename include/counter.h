#ifndef BONSAI_COUNTER_H
#define BONSAI_COUNTER_H

#include "arch.h"

#include <stdlib.h>

struct counter {
    int nr_ino;
    int nr_pno;
    size_t index_mem;
} ____cacheline_aligned;

extern struct counter counters[];
extern atomic_t nr_thread_counter;
struct counter *this_counter();

#define COUNTER_ADD(name, delta)   this_counter()->name += (delta)
#define COUNTER_SUB(name, delta)   this_counter()->name -= (delta)
#define COUNTER_INC(name)          this_counter()->name++
#define COUNTER_DEC(name)          this_counter()->name--
#define COUNTER_GET(name)          ({                           \
    uint32_t cp;                                                \
    typeof(counters[0].name) tot = 0;                           \
    for (cp = 0; cp < atomic_read(&nr_thread_counter); cp++) {  \
        tot += counters[cp].name;                               \
    }                                                           \
    tot;                                                        \
})

#endif //BONSAI_COUNTER_H
