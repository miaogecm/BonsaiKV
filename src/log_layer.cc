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
