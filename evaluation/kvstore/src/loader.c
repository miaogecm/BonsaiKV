#include <stdio.h>
#include <dlfcn.h>
#include <assert.h>

#include "loader.h"

static inline void find_kvop(void **opp, void *handle, const char *name) {
    const char *error;
    *opp = dlsym(handle, name);
    if ((error = dlerror())) {
        printf("find_kvop: error %s\n", error);
        assert(0);
    }
}

void load_kvstore(struct kvstore *kvstore, const char *libpath) {
    void *handle = dlopen(libpath, RTLD_LAZY);
    if (!handle) {
        assert(0);
    }
    find_kvop((void **) &kvstore->kv_engine, handle, "kv_engine");
    find_kvop((void **) &kvstore->kv_create_context, handle, "kv_create_context");
    find_kvop((void **) &kvstore->kv_destroy_context, handle, "kv_destroy_context");
    find_kvop((void **) &kvstore->kv_start_test, handle, "kv_start_test");
    find_kvop((void **) &kvstore->kv_stop_test, handle, "kv_stop_test");
    find_kvop((void **) &kvstore->kv_thread_create_context, handle, "kv_thread_create_context");
    find_kvop((void **) &kvstore->kv_thread_destroy_context, handle, "kv_thread_destroy_context");
    find_kvop((void **) &kvstore->kv_thread_start_test, handle, "kv_thread_start_test");
    find_kvop((void **) &kvstore->kv_thread_stop_test, handle, "kv_thread_stop_test");
    find_kvop((void **) &kvstore->kv_txn_begin, handle, "kv_txn_begin");
    find_kvop((void **) &kvstore->kv_txn_abort, handle, "kv_txn_abort");
    find_kvop((void **) &kvstore->kv_txn_commit, handle, "kv_txn_commit");
    find_kvop((void **) &kvstore->kv_put, handle, "kv_put");
    find_kvop((void **) &kvstore->kv_del, handle, "kv_del");
    find_kvop((void **) &kvstore->kv_get, handle, "kv_get");
    find_kvop((void **) &kvstore->kv_scan, handle, "kv_scan");
}
