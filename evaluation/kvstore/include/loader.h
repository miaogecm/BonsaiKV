#ifndef BONSAI_LOADER_H
#define BONSAI_LOADER_H

#include <unistd.h>

struct kvstore {
    const char *(*kv_engine)();
    void *(*kv_create_context)(void *config);
    void (*kv_destroy_context)(void *context);
    void (*kv_start_test)(void *context);
    void (*kv_stop_test)(void *context);
    void *(*kv_thread_create_context)(void *context, int id);
    void (*kv_thread_destroy_context)(void *tcontext);
    void (*kv_thread_start_test)(void *tcontext);
    void (*kv_thread_stop_test)(void *tcontext);
    void (*kv_txn_begin)(void *tcontext);
    void (*kv_txn_rollback)(void *tcontext);
    void (*kv_txn_commit)(void *tcontext);
    int (*kv_put)(void *tcontext, void *key, size_t key_len, void *val, size_t val_len);
    int (*kv_del)(void *tcontext, void *key, size_t key_len);
    int (*kv_get)(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len);
    void (*kv_scan)(void *tcontext, void *key, size_t key_len, int range, void *values);
};

void load_kvstore(struct kvstore *kvstore, const char *libpath);

#endif //BONSAI_LOADER_H
