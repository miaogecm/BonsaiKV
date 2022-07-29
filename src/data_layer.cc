/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
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
