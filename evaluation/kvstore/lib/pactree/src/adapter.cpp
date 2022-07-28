#include "pactree.h"

#define NUM_SOCKET 2

extern "C" {

const char *kv_engine() {
    return "pactree";
}

void *kv_create_context(void *config) {
    auto *db = new pactree(NUM_SOCKET);
    return db;
}

void kv_destroy_context(void *context) {
    auto *db = static_cast<pactree *>(context);
    delete db;
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
}

void *kv_thread_create_context(void *context, int id) {
    auto *db = static_cast<pactree *>(context);
    db->registerThread();
    return db;
}

void kv_thread_destroy_context(void *tcontext) {
    auto *db = static_cast<pactree *>(tcontext);
    db->unregisterThread();
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

static inline Key_t get_key(void *key, size_t key_len) {
#ifdef STRINGKEY
    Key_t k;
    k.set(static_cast<const char *>(key), key_len);
    return k;
#else
    assert(key_len == sizeof(unsigned long));
    return __builtin_bswap64(*(unsigned long *) key);
#endif
}

static inline Val_t get_val(void *val, size_t val_len) {
    assert(val_len == sizeof(unsigned long));
    return *(unsigned long *) val;
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *db = static_cast<pactree *>(tcontext);
    db->insert(get_key(key, key_len), get_val(val, val_len));
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    auto *db = static_cast<pactree *>(tcontext);
    db->remove(get_key(key, key_len));
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *db = static_cast<pactree *>(tcontext);
    Key_t key_ = get_key(key, key_len);
    Val_t val_ = db->lookup(key_);
    *(unsigned long *) val = val_;
    *val_len = sizeof(unsigned long);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    auto *db = static_cast<pactree *>(tcontext);
    Key_t key_ = get_key(key, key_len);
    std::vector<Val_t> res;
    db->scan(key_, range, res);
}

}
