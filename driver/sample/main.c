#include <stdio.h>

#include "skiplist.c"
#include "tpcc.h"

void* s_init() {
    return (void*) sl_init();
}

void s_destroy(void* index_struct) {
    sl_entry* sl = (sl_entry*) index_struct;
    sl_destroy(index_struct);
}

int s_insert(void* index_struct, const void *key, size_t len, const void* value, size_t v_len) {
    sl_entry* sl = (sl_entry*) index_struct;
    sl_set(sl, (char*) key, (char*) value);
}

int s_update(void* index_struct, const void *key, size_t len, const void* value, size_t v_len) {
    sl_entry* sl = (sl_entry*) index_struct;
    sl_set(sl, (char*) key, (char*) value);
}

int s_remove(void* index_struct, const void *key, size_t len) {
    sl_entry* sl = (sl_entry*) index_struct;
    sl_unset(sl, (char*) key);
}

void* s_lookup(void* index_struct, const void *key, size_t len) {
    sl_entry* sl = (sl_entry*) index_struct;
    return sl_get(sl, (char*) key);
}

int s_scan(void* index_struct, const void *low, const void *high, size_t len, size_t* arr) {
    sl_entry* sl = (sl_entry*) index_struct;
    return sl_scan(sl, (char*) low, (char*) high, arr);
}

int main() {
    tpcc_init(s_init, s_destroy, s_insert, s_update, s_remove, s_lookup, s_scan);
    tpcc_set_env(1, 100000, 6);
    tpcc_load(1);
    tpcc_bench(1);
    tpcc_destroy();
}