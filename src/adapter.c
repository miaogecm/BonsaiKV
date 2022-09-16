/*
 * BonsaiKV: Towards Fast, Scalable, and Persistent Key-Value Stores with Tiered, Heterogeneous Memory System
 *
 * Adapter
 */

#include <stdlib.h>
#include <errno.h>

#include "bonsai.h"
#include "masstree.h"
#include "counter.h"

#define INT2KEY(val)        (* (pkey_t *) (unsigned long []) { (val) })

struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
    int stm_support;
};

static void *masstree_alloc(size_t size) {
    COUNTER_ADD(index_mem, size);
    return malloc(size);
}

static void masstree_free(void *p, size_t size) {
    COUNTER_SUB(index_mem, size);
    free(p);
}

static void *index_init() {
    static masstree_ops_t ops = { .alloc = masstree_alloc, .free = masstree_free };
    return (void*) masstree_create(&ops);
}

static void index_destory(void* index_struct) {
    masstree_t* tr = (masstree_t*) index_struct;
    masstree_destroy(tr);
}

static int index_insert(void* index_struct, const void *key, size_t len, const void *value) {
    masstree_t* tr = (masstree_t*) (index_struct);
    masstree_put(tr, key, len, (void*) value);

    return 0;
}

static int index_update(void* index_struct, const void *key, size_t len, const void* value) {
    masstree_t* tr = (masstree_t*) (index_struct);
    masstree_put(tr, key, len, (void*) value);
    
    return 0;
}

static int index_remove(void* index_struct, const void *key, size_t len) {
    masstree_t* tr = (masstree_t*) (index_struct);
    return masstree_del(tr, key, len) ? 0 : -ENOENT;
}

static void* index_lookup(void* index_struct, const void *key, size_t len, const void *actual_key) {
    masstree_t* tr = (masstree_t*) (index_struct);
    return masstree_get(tr, key, len, actual_key);
}

static void* index_lowerbound(void* index_struct, const void *key, size_t len, const void *actual_key) {
    masstree_t* tr = (masstree_t*) (index_struct);
    return masstree_get(tr, key, len, actual_key);
}

static int index_scan(void* index_struct, const void *min, const void *max) {
    return 0;
}

extern int
bonsai_init(char *index_name, init_func_t init, destory_func_t destory, insert_func_t insert, update_func_t update,
            remove_func_t remove, lookup_func_t lookup, scan_func_t scan);
extern void bonsai_deinit();

extern void bonsai_mark_cpu(int cpu);
extern void bonsai_barrier();

extern void bonsai_online();
extern void bonsai_offline();

extern pkey_t bonsai_make_key(const void *key, size_t len);

extern int bonsai_insert(pkey_t key, pval_t value);
extern int bonsai_remove(pkey_t key);
extern int bonsai_insert_commit(pkey_t key, pval_t value);
extern int bonsai_remove_commit(pkey_t key);
extern int bonsai_lookup(pkey_t key, pval_t *val);
extern int bonsai_scan(pkey_t start, int range, pval_t *values);

extern void bonsai_dtx_start();
extern void bonsai_dtx_commit();

extern int bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

size_t bonsai_get_dram_usage();

const char *kv_engine() {
    return "bonsai";
}

void *kv_create_context(void *config) {
    struct bonsai_config *bonsai_config = config;
    int i;
    for (i = 0; i < bonsai_config->nr_user_cpus; i++) {
        bonsai_mark_cpu(bonsai_config->user_cpus[i]);
    }
    bonsai_init("masstree",
                index_init, index_destory, index_insert, index_update, index_remove, index_lowerbound, index_scan);
    return NULL;
}

void kv_destroy_context(void *context) {
    assert(context == NULL);
    bonsai_deinit();
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
    bonsai_barrier();

    printf("===== Counters =====\n");
    printf("index memory: %lu bytes\n", COUNTER_GET(index_mem));
    printf("nr_ino: %d\n", COUNTER_GET(nr_ino));
    printf("nr_pno: %d\n", COUNTER_GET(nr_pno));
    printf("====================\n");

    printf("Bonsai DRAM Usage: %lu bytes\n", bonsai_get_dram_usage());
}

void *kv_thread_create_context(void *context, int id) {
    assert(context == NULL);
    bonsai_user_thread_init(pthread_self());
    return NULL;
}

void kv_thread_destroy_context(void *tcontext) {
    assert(tcontext == NULL);
    bonsai_user_thread_exit();
}

void kv_thread_start_test(void *tcontext) {
    assert(tcontext == NULL);
    bonsai_online();
}

void kv_thread_stop_test(void *tcontext) {
    assert(tcontext == NULL);
    bonsai_offline();
}

void kv_txn_begin(void *tcontext) {
    assert(0);
}

void kv_txn_rollback(void *tcontext) {
    assert(0);
}

void kv_txn_commit(void *tcontext) {
    assert(0);
}

static inline pkey_t get_pkey(void *key, size_t key_len) {
#ifdef STR_KEY
    assert(key_len <= KEY_LEN);
    return bonsai_make_key(key, key_len);
#else
    assert(key_len == sizeof(unsigned long));
    return INT2KEY(__builtin_bswap64(*(unsigned long *) key));
#endif
}

static inline pval_t get_pval(void *val, size_t val_len) {
#ifdef STR_VAL
    uint8_t val_[VAL_LEN] = { 0 };
    assert(val_len <= VAL_LEN);
    memcpy(val_, val, val_len);
    return bonsai_make_val(VCLASS, val_);
#else
    assert(val_len == sizeof(unsigned long));
    return *(unsigned long *) val;
#endif
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    pkey_t pkey = get_pkey(key, key_len);
    assert(tcontext == NULL);
    return bonsai_insert_commit(pkey, get_pval(val, val_len));
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    pkey_t pkey = get_pkey(key, key_len);
    assert(tcontext == NULL);
    return bonsai_remove_commit(pkey);
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    pkey_t pkey = get_pkey(key, key_len);
    pval_t pval;
    int ret;
    assert(tcontext == NULL);
    ret = bonsai_lookup(pkey, &pval);
    if (likely(!ret)) {
        valman_free_v(pval);
    }
    return ret;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    pkey_t pkey = get_pkey(key, key_len);
    assert(tcontext == NULL);
    bonsai_scan(pkey, range, values);
}
