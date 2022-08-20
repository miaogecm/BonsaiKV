#include <stdint.h>
#include <stdlib.h>

#include "masstree.h"

const char *kv_engine() {
    return "masstree-rmind";
}

void *kv_create_context(void *config) {
    return masstree_create(NULL);
}

void kv_destroy_context(void *context) {
    masstree_destroy(context);
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
}

void *kv_thread_create_context(void *context, int id) {
    return context;
}

void kv_thread_destroy_context(void *tcontext) {
}

void kv_thread_start_test(void *tcontext) {
}

void kv_thread_stop_test(void *tcontext) {
}

void kv_txn_begin(void *tcontext) {
    assert(0);
}

void kv_txn_commit(void *tcontext) {
    assert(0);
}

void kv_txn_rollback(void *tcontext) {
    assert(0);
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    masstree_t *mt = tcontext;
    masstree_put(mt, key, key_len, val);
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    masstree_t *mt = tcontext;
    masstree_del(mt, key, key_len);
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    masstree_t *mt = tcontext;
    volatile void *res = masstree_get(mt, key, key_len, NULL);
    res;
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    assert(0);
}
