#include "tree.h"
#include "lbtree-src/lbtree.h"

struct lbtree_config {
    int nr_workers;
};

const char *kv_engine() {
    return "lbtree";
}

void initUseful(void);

void *kv_create_context(void *config) {
    auto *cfg = static_cast<lbtree_config *>(config);

    printf("NON_LEAF_KEY_NUM= %d, LEAF_KEY_NUM= %d, nonleaf size= %lu, leaf size= %lu\n",
           NON_LEAF_KEY_NUM, LEAF_KEY_NUM, sizeof(bnode), sizeof(bleaf));
    assert((sizeof(bnode) == NONLEAF_SIZE)&&(sizeof(bleaf) == LEAF_SIZE));

    initUseful();

    // initialize nvm pool and log per worker thread
    the_thread_nvmpools.init(cfg->nr_workers, "/mnt/ext4/dimm0/lbtree", 64 * 1024 * 1024 * 1024ul);

    // allocate a 4KB page for the tree in worker 0's pool
    char *nvm_addr= (char *)nvmpool_alloc(4*KB);
    the_treep= initTree(nvm_addr, false);

    // log may not be necessary for some tree implementations
    // For simplicity, we just initialize logs.  This cost is low.
    nvmLogInit(worker_thread_num);

    return the_treep;
}

void kv_destroy_context(void *context) {
    delete (tree *) context;
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
}

void *kv_thread_create_context(void *context, int id) {
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

static inline key_type get_key(void *key, size_t key_len) {
    assert(key_len == sizeof(uint64_t));
    return __builtin_bswap64(*(unsigned long *) key);
}

static inline void *get_val(void *val, size_t val_len) {
    assert(val_len == sizeof(uint64_t));
    return (void *) *(unsigned long *) val;
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    tree *tree = static_cast<class tree *>(tcontext);
    tree->insert(get_key(key, key_len), get_val(val, val_len));
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    tree *tree = static_cast<class tree *>(tcontext);
    tree->del(get_key(key, key_len));
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    tree *tree = static_cast<class tree *>(tcontext);
    int pos;
    void *ptr = tree->lookup(get_key(key, key_len), &pos);
    asm(""::"r"(ptr):);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    assert(0);
}
