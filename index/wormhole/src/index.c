#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "bench.h"
#include "lib.h"
#include "kv.h"
#include "wh.h"

// #define LONG_KEY

struct wormhole* wh;

static void *_wh_init() {
    wh = wh_create();
    struct wormref* ref = wh_ref(wh);
    return (void*) ref;
}

static void _wh_destory(void* index_struct) {
    struct wormref* ref = (struct wormref*) index_struct;
    
    // wh_unref(ref);

    wh_destroy(wh);
}

static int _wh_insert(void* index_struct, pkey_t key, size_t len, void* val) {
    struct wormref* ref = (struct wormref*) index_struct;
    pkey_t r_key, __key;

#ifndef LONG_KEY
    r_key = __builtin_bswap64(*(pkey_t*)key);
    __key = &r_key;
#else
    __key = key;
#endif
    wh_put(ref, __key, len, &val, sizeof(val));
    
    return 0;
}

static int _wh_update(void* index_struct, pkey_t key, size_t len, void* val) {
    struct wormref* ref = (struct wormref*) index_struct;
    pkey_t r_key, __key;

#ifndef LONG_KEY
    r_key = __builtin_bswap64(*(pkey_t*)key);
    __key = &r_key;
#else
    __key = key;
#endif
    wh_put(ref, __key, len, &val, sizeof(val));
    
    return 0;
}

static int _wh_remove(void* index_struct, pkey_t key, size_t len) {
    struct wormref* ref = (struct wormref*) index_struct;
    pkey_t r_key, __key;

#ifndef LONG_KEY
    r_key = __builtin_bswap64(*(pkey_t*)key);
    __key = &r_key;
#else
    __key = key;
#endif
    wh_del(ref, __key, len);

    return 0;
}

static void* _wh_lookup(void* index_struct, pkey_t key, size_t len) {
    struct wormref* ref = (struct wormref*) index_struct;
    struct wormhole_iter* iter = wh_iter_create(ref);
    pkey_t r_key, __key;

#ifndef LONG_KEY
    r_key = __builtin_bswap64(*(pkey_t*)key);
    __key = &r_key;
#else
    __key = key;
#endif

    wh_iter_seek(iter, __key, len);
    assert(wh_iter_valid(iter));

    u32 len_out;
    pval_t val_out;
    wh_iter_peek(iter, NULL, 0, NULL, &val_out, sizeof(pval_t), &len_out);

    return (void*) val_out;
}

static void* _wh_lowerbound(void* index_struct, pkey_t key, size_t len) {
    struct wormref* ref = (struct wormref*) index_struct;
    struct wormhole_iter* iter = wh_iter_create(ref);

    pkey_t __key, r_key;

#ifndef LONG_KEY
    r_key = __builtin_bswap64(*(pkey_t*)key);
    __key = &r_key;
#else
    __key = key;
#endif

    wh_iter_seek(iter, __key, len);
    assert(wh_iter_valid(iter));

    u32 len_out;
    pval_t val_out;
    wh_iter_peek(iter, NULL, 0, NULL, &val_out, sizeof(pval_t), &len_out);

    return (void*) val_out;
}

static int _wh_scan(void* index_struct, pkey_t min, pkey_t max) {
    return 0;
}

// void kv_print() {
// }

int main() {
    return bench("worehole", _wh_init, _wh_destory, _wh_insert, _wh_update, _wh_remove, _wh_lookup, _wh_lowerbound, _wh_scan);
}