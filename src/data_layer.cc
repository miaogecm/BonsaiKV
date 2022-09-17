/*
 * BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System
 *
 * Data Layer: Scalable data layout organization
 */

#include <algorithm>
#include "bonsai.h"

extern "C" {

void sort_perm_arr(uint8_t *perm, pentry_t *base, int n) {
    /* STL sort is a lot faster than glibc sort... */
    for (int i = 0; i < n; i++) {
        perm[i] = i;
    }
    std::sort(perm, perm + n, [base] (const uint8_t &v1, const uint8_t &v2) {
        return pkey_compare(base[v1].k, base[v2].k) < 0;
    });
}

}
