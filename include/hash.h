#ifndef _HASH_H
#define _HASH_H

#include <stdint.h>

static inline uint32_t hash(uint64_t x, int64_t* seeds) {
    uint32_t low = (uint32_t) x;
    uint32_t high = (uint32_t)(x >> 32);
    return (uint32_t)((seeds[0] * low + seeds[1] * high + seeds[2]) >> 32);
}

static inline uint64_t phash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

#endif
/*hash.h*/