/*
 * Intel PMEM Performance Test
 */

#define _GNU_SOURCE

#include <libpmem.h>
#include <assert.h>

static char *pool_paths[] = {
        "/mnt/ext4/dimm0/nvm_perf_test",
};

void *nvm_create_pool(int id, size_t size) {
    size_t mapped_len;
    void *p;
    int is;

    p = pmem_map_file(pool_paths[id], size, PMEM_FILE_CREATE, 0666, &mapped_len, &is);
    assert(p);
    assert(is);
    assert(mapped_len == size);

    return p;
}
