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

struct wormhole* wh;

static void *_wh_init() {
    wh = wh_create();
    struct wormref* ref = wh_ref(wh);
    return (void*) ref;
}

static void _wh_destory(void* index_struct) {
    struct wormref* ref = (struct wormref*) index_struct;
    
    wh_unref(ref);

    wh_destroy(wh);
}

static int _wh_insert(void* index_struct, pkey_t key, void* value) {
    struct wormref* ref = (struct wormref*) index_struct;

    pkey_t r_key = __builtin_bswap64(key);
    pval_t r_val = __builtin_bswap64(value);
    wh_put(ref, &r_key, sizeof(r_key), &r_val, sizeof(r_val));
    
    return 0;
}

static int _wh_remove(void* index_struct, pkey_t key) {
    struct wormref* ref = (struct wormref*) index_struct;

    pkey_t r_key = __builtin_bswap64(key);
    wh_del(ref, &r_key, sizeof(r_key));

    return 0;
}

static void* _wh_lookup(void* index_struct, pkey_t key) {
    struct wormref* ref = (struct wormref*) index_struct;

    struct wormhole_iter* iter = wh_iter_create(ref);
    pkey_t r_key = __builtin_bswap64(key);

    wh_iter_seek(iter, &r_key, sizeof(r_key));
    assert(wh_iter_valid(iter));

    u32 len_out;
    pval_t val_out;
    wh_iter_peek(iter, NULL, 0, NULL, &val_out, sizeof(pval_t), &len_out);

    return (void*) val_out;
}

static void* _wh_lowerbound(void* index_struct, pkey_t key) {
    struct wormref* ref = (struct wormref*) index_struct;

    struct wormhole_iter* iter = wh_iter_create(ref);
    pkey_t r_key = __builtin_bswap64(key);

    wh_iter_seek(iter, &r_key, sizeof(r_key));
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
    return bench("worehole", _wh_init, _wh_destory, _wh_insert, _wh_remove, _wh_lookup, _wh_lowerbound, _wh_scan);
}