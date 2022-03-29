/*
 * Intel PMEM Performance Test
 */

#ifndef NVM_PERF_MEASURE_H
#define NVM_PERF_MEASURE_H

#include <stddef.h>
#include <sys/time.h>

static __thread struct timeval t0, t1;

static inline void start_measure() {
    gettimeofday(&t0, NULL);
}

static inline double end_measure() {
    gettimeofday(&t1, NULL);
    return t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
}

#endif //NVM_PERF_MEASURE_H
