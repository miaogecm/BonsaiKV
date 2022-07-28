#include "concur_dptree.hpp"
#include <string>

int parallel_merge_worker_num = 24;

typedef dptree::concur_dptree<uint64_t, uint64_t> DB;

extern "C" {

const char *kv_engine() {
    return "dptree";
}

void *kv_create_context(void *config) {
    auto *db = new DB;
    return db;
}

void kv_destroy_context(void *context) {
    auto *db = static_cast<DB *>(context);
    delete db;
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

static inline uint64_t get_key(void *key, size_t key_len) {
    assert(key_len == sizeof(uint64_t));
    return __builtin_bswap64(*(unsigned long *) key);
}

static inline uint64_t get_val(void *val, size_t val_len) {
    assert(val_len == sizeof(uint64_t));
    return *(unsigned long *) val;
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *client = static_cast<DB *>(tcontext);
    client->insert(get_key(key, key_len), get_val(val, val_len));
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    assert(0);
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *client = static_cast<DB *>(tcontext);
    bool found = client->lookup(get_key(key, key_len), *(uint64_t *) val);
    assert(found);
    *val_len = sizeof(uint64_t);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    auto *db = static_cast<DB *>(tcontext);
    std::vector<uint64_t> vals;
    uint64_t key_ = get_key(key, key_len);
    db->lookup_range(key_, range, vals);
}

}
