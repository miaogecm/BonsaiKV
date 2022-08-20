#include <algorithm>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#define GLOBAL_VALUE_DEFINE
#include "masstree_wrapper.hpp"

using ValueType = void;
using MT = MasstreeWrapper<ValueType>;

extern "C" {

const char *kv_engine() {
    return "masstree-kohler";
}

void *kv_create_context(void *config) {
    return new MT;
}

void kv_destroy_context(void *context) {
    delete static_cast<MT *>(context);
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
}

void *kv_thread_create_context(void *context, int id) {
    MT *mt = static_cast<MT *>(context);
    mt->thread_init(id);
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
    auto *client = static_cast<MT *>(tcontext);
    client->insert_value(static_cast<const char *>(key), key_len, val);
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    auto *client = static_cast<MT *>(tcontext);
    client->remove_value(static_cast<const char *>(key), key_len);
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *client = static_cast<MT *>(tcontext);
    volatile const void *res;
    client->rscan("\0", 1, false, static_cast<const char *>(key), key_len, false, {
        [](const MT::leaf_type *leaf, uint64_t version, bool &continue_flag) {},
        [&res](const MT::Str &key, const ValueType *val, bool &continue_flag) {
            res = val;
        }
    }, 1);
    res;
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    assert(0);
}

}
