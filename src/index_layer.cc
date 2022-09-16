/*
 * BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System
 *
 * Index Layer
 */

#include <algorithm>
#include "bonsai.h"

extern "C" {

void sort_log_info(struct log_info *logs, int n) {
    /* STL sort is a lot faster than glibc sort... */
    std::sort(logs, logs + n, [] (const struct log_info &v1, const struct log_info &v2) {
        return pkey_compare(v1.oplog->o_kv.k, v2.oplog->o_kv.k) < 0;
    });
}

}
