#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "bench.h"
#include "masstree.h"

static void *mass_init() {
    return (void*) masstree_create(NULL);
}

static void mass_destory(void* index_struct) {
    masstree_t* tr = (masstree_t*) index_struct;
    masstree_destroy(tr);
}

static int mass_insert(void* index_struct, pkey_t key, void* value) {
    masstree_t* tr = (masstree_t*) (index_struct);
    masstree_put(tr, &key, sizeof(key), (void*) value);
    
    return 0;
}

static int mass_remove(void* index_struct, pkey_t key) {
    masstree_t* tr = (masstree_t*) (index_struct);
    masstree_del(tr, &key, sizeof(key));

    return 0;
}

static void* mass_lookup(void* index_struct, pkey_t key) {
    masstree_t* tr = (masstree_t*) (index_struct);
    return masstree_get(tr, &key, sizeof(key));
}

static void* mass_lowerbound(void* index_struct, pkey_t key) {
    masstree_t* tr = (masstree_t*) (index_struct);
    return masstree_get(tr, &key, sizeof(key));
}

static int mass_scan(void* index_struct, pkey_t min, pkey_t max) {
    return 0;
}

void kv_print() {
}

int main() {
    return bench("masstree", mass_init, mass_destory, mass_insert, mass_remove, mass_lookup, mass_lowerbound, mass_scan);
}