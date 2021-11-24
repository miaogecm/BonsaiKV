#ifndef _CMP_H
#define _CMP_H

#include <stdint.h>

static int cmp(uint64_t a, uint64_t b) {
    if (a < b) return -1
    if (a > b) return 1;
    return 0;
}

#endif
/*cmp.h*/