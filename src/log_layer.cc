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

void sort_oplogs(struct oplog *oplogs, int nr) {
    /* STL sort is a lot faster than glibc sort... */
    std::sort(oplogs, oplogs + nr, [](const struct oplog &v1, const struct oplog &v2) {
        return oplog_cmp(&v1, &v2) < 0;
    });
}

}