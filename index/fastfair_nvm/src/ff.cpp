#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "bench.h"
#include "btree.h"

static inline int file_exists(char const *file) { return access(file, F_OK); }

static char* persistent_path = "/mnt/ext4/pool";

TOID(btree) bt = TOID_NULL(btree);
PMEMobjpool *pop;

void init_pmdk() {
  if (file_exists(persistent_path) != 0) {
    pop = pmemobj_create(persistent_path, "btree", 1000000000UL,
                         0666); // make 1GB memory pool
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
  } else {
    pop = pmemobj_open(persistent_path, "btree");
    bt = POBJ_ROOT(pop, btree);
    printf("pool existed\n");
    exit(1);
  }
}

static void *ff_init() {
    init_pmdk();
    return D_RW(bt);
}

static void ff_destory(void* index_struct) {
}

static int ff_insert(void* index_struct, pkey_t key, void* value) {
    auto* ff = static_cast<btree *>(index_struct);
    ff->btree_insert(key, (char*) value);
    return 0;
}

static int ff_remove(void* index_struct, pkey_t key) {
    auto* ff = static_cast<btree *>(index_struct);
    ff->btree_delete(key);
    return 0;
}

static void* ff_lookup(void* index_struct, pkey_t key) {
    auto* ff = static_cast<btree *>(index_struct);
    return ff->btree_search(key);
}

static void* ff_lowerbound(void* index_struct, pkey_t key) {
    assert(0);
}

static int ff_scan(void* index_struct, pkey_t min, pkey_t max) {
    return 0;
}

extern "C" {
void kv_print() {
}
}

int main() {
    return bench("fastfair_nvm", ff_init, ff_destory, ff_insert, ff_remove, ff_lookup, ff_lowerbound, ff_scan);
}