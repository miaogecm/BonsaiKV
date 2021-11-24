#ifndef _HASH_H
#define _HASH_H

#include <stdint.h>

uint32_t hash(uint64_t x, int64_t* seeds) {
    uint32_t low = (uint32_t) x;
    uint32_t high = (uint32_t)(x >> 32);
    return (uint32_t)((seeds[0] * low + seeds[1] * high + seeds[2]) >> 32);
}

#endif
/*hash.h*/