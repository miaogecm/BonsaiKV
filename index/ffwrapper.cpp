#include "ffwrapper.h"
#include "fast_fair.h"

#ifdef __cplusplus
extern "C" {
#endif

struct fast_fair {
    btree tree;
};

struct fast_fair* ff_init() {
    return new struct fast_fair;
}

void ff_destory(void* index_struct) {
    struct fast_fair* ff = (struct fast_fair*) index_struct;
    delete ff;
}

int ff_insert(void* index_struct, pkey_t key, void* value) {
    struct fast_fair* ff = (struct fast_fair*) index_struct;
    return ff->tree.btree_insert(key, (char*) value);
}

int ff_update(void* index_struct, pkey_t key, void* value) {
    struct fast_fair* ff = (struct fast_fair*) index_struct;
    return ff->tree.btree_insert(key, (char*) value);
}

int ff_remove(void* index_struct, pkey_t key) {
    struct fast_fair* ff = (struct fast_fair*) index_struct;
    return ff->tree.btree_delete(key);
}

void* ff_lookup(void* index_struct, pkey_t key) {
    struct fast_fair* ff = (struct fast_fair*) index_struct;
    return ff->tree.btree_search(key);
}

int ff_scan(void* index_struct, pkey_t min, pkey_t max) {
    return 0;
}

#ifdef __cplusplus
};
#endif