#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "bench.h"
#include "btree.h"

static void *ff_init() {
    return new btree;
}

static void ff_destory(void* index_struct) {
    delete static_cast<btree *>(index_struct);
}

static int ff_insert(void* index_struct, pkey_t key, void* value) {
    auto* ff = static_cast<btree *>(index_struct);
    ff->btree_insert(key, (char*) value);
    return 0;
}

static int ff_update(void* index_struct, pkey_t key, void* value) {
    auto* ff = static_cast<btree *>(index_struct);
    ff->btree_update(key, (char*) value);
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
    auto* ff = static_cast<btree *>(index_struct);
    return ff->btree_lowerbound(key);
}

static int ff_scan(void* index_struct, pkey_t min, pkey_t max) {
    return 0;
}

extern "C" {
void kv_print() {
}
}

int main() {
    return bench("fastfair_dram", ff_init, ff_destory, ff_insert, ff_update, ff_remove, ff_lookup, ff_lowerbound, ff_scan);
}