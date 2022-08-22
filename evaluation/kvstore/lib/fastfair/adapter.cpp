#include "btree.h"

static const char *persistent_path = "/mnt/ext4/dimm0/fastfair";

static inline int file_exists(char const *file) {
    return access(file, F_OK);
}

#define POOL_SIZE   (64 * 1024ul * 1024ul * 1024ul)

extern "C" {

const char *kv_engine() {
    return "fastfair";
}

void *kv_create_context(void *config) {
    TOID(btree) bt = TOID_NULL(btree);
    PMEMobjpool *pop;
    pop = pmemobj_create(persistent_path, "btree", POOL_SIZE, 0666);
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
    return D_RW(bt);
}

void kv_destroy_context(void *context) {
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

static inline entry_key_t get_key(void *key, size_t key_len) {
    assert(key_len == sizeof(unsigned long));
    return __builtin_bswap64(*(unsigned long *) key);
}

static inline char *get_val(void *val, size_t val_len) {
    assert(val_len == sizeof(unsigned long));
    return *(char **) val;
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *db = static_cast<btree *>(tcontext);
    db->btree_insert(get_key(key, key_len), get_val(val, val_len));
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    auto *db = static_cast<btree *>(tcontext);
    db->btree_delete(get_key(key, key_len));
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *db = static_cast<btree *>(tcontext);
    entry_key_t key_ = get_key(key, key_len);
    char *val_ = db->btree_search(key_);
    *(unsigned long *) val = (unsigned long) val_;
    *val_len = sizeof(unsigned long);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    auto *db = static_cast<btree *>(tcontext);
    entry_key_t key_ = get_key(key, key_len);
    std::vector<uint64_t> vec;
    db->btree_search_range(key_, range, vec);
}

}
