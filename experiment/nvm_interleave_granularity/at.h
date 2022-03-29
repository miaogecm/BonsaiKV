/*
 * Intel PMEM Performance Test
 */

#ifndef NVM_INTERLEAVE_GRANULARITY_AT_H
#define NVM_INTERLEAVE_GRANULARITY_AT_H

#include <stdint.h>
#include "arch.h"
#include "config.h"

typedef size_t va_t;

struct at_policy {
    void *pa_bases[CONFIG_DIMM_CNT];
};

static inline void *va2pa(struct at_policy *policy, va_t va) {
    unsigned int vchunk = va / CONFIG_CHUNK_SIZE;
    unsigned int dimm_idx = vchunk % CONFIG_DIMM_CNT;
    unsigned int pchunk = vchunk / CONFIG_DIMM_CNT;
    size_t off = va % CONFIG_CHUNK_SIZE;
    return policy->pa_bases[dimm_idx] + (pchunk * CONFIG_CHUNK_SIZE) + off;
}

#endif //NVM_INTERLEAVE_GRANULARITY_AT_H
