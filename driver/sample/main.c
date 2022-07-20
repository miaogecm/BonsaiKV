#include <stdio.h>

#include "skiplist.c"
#include "bench.h"
#include "spinlock.h"

void* s_init() {
    return (void*) sl_init();
}

void s_destroy(void* index_struct) {
    sl_entry* sl = (sl_entry*) index_struct;
    sl_destroy(sl);
}

int s_insert(void* index_struct, const void *key, size_t len, const void* value, size_t v_len) {
    sl_entry* sl = (sl_entry*) index_struct;
    spinlock_t lock;

    spin_lock_init(&lock);    
    spin_lock(&lock);

    sl_set(sl, (char*) key, (char*) value, len, v_len);

    spin_unlock(&lock);
    return 0;
}

int s_update(void* index_struct, const void *key, size_t len, const void* value, size_t v_len) {
    sl_entry* sl = (sl_entry*) index_struct;
    spinlock_t lock;

    spin_lock_init(&lock);    
    spin_lock(&lock);

    sl_set(sl, (char*) key, (char*) value, len, v_len);
    
    spin_unlock(&lock);
    return 0;
}

int s_remove(void* index_struct, const void *key, size_t len) {
    sl_entry* sl = (sl_entry*) index_struct;
    spinlock_t lock;

    spin_lock_init(&lock);    
    spin_lock(&lock);

    sl_unset(sl, (char*) key, len);

    spin_unlock(&lock);
    return 0;
}

void* s_lookup(void* index_struct, const void *key, size_t len) {
    sl_entry* sl = (sl_entry*) index_struct;
    void* ret;
    spinlock_t lock;

    spin_lock_init(&lock);    
    spin_lock(&lock);

    ret = sl_get(sl, (char*) key, len);

    spin_unlock(&lock);
    return ret;
}

int s_scan(void* index_struct, const void *low, const void *high, size_t len, size_t* k_arr, size_t* v_arr) {
    sl_entry* sl = (sl_entry*) index_struct;
    int ret;
    spinlock_t lock;

    spin_lock_init(&lock);    
    spin_lock(&lock);

    ret = sl_scan(sl, (char*) low, (char*) high, k_arr, v_arr, len);

    spin_unlock(&lock);
    return ret;
}

int main() {
    bench_init(s_init, s_destroy, s_insert, s_update, s_remove, s_lookup, s_scan);
    bench_set_env(20, 100000, 48);
    bench_load(20);
    run_bench(20);
    bench_destroy();
}