#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "loader.h"
#include "runner.h"
#include "compressor.h"
#include "decompressor.h"
#include "config.h"

static int str_key, str_val;
static ycsb_decompressor_t load_dec, op_dec;

#define STRINGIFY(x) #x
#define TOSTRING(x)  STRINGIFY(x)

#define LOAD_PATH   "tools/index-microbench/workloads/" YCSB_WORKLOAD_NAME "/" TOSTRING(NUM_THREADS) "/load"
#define OP_PATH     "tools/index-microbench/workloads/" YCSB_WORKLOAD_NAME "/" TOSTRING(NUM_THREADS) "/op"

#define VAL_LEN     YCSB_VAL_LEN
static char valbuf[VAL_LEN];
static __thread char valres[VAL_LEN];

static void *get_val(size_t *len) {
    *len = VAL_LEN;
    return valbuf;
}

static void do_op(struct kvstore *kvstore, void *tcontext, ycsb_decompressor_t *dec, long id) {
    long i, repeat = 1;
    int st, ed;
    enum op_type op;
    int ret, nr, range;
    uint64_t int_key;
    void *key, *val;
    size_t key_len, val_len;
    uint64_t values[1024];

    nr = ycsb_decompressor_get_nr(dec);

    st = 1.0 * id / NUM_THREADS * nr;
    ed = 1.0 * (id + 1) / NUM_THREADS * nr;

    while(repeat--) {
        for (i = st; i < ed; i ++) {
            if (str_key) {
                op = ycsb_decompressor_get(dec, &key, &range, i);
                key_len = STR_KEY_LEN;
            } else {
                op = ycsb_decompressor_get(dec, &int_key, &range, i);
                int_key = __builtin_bswap64(int_key);
                key = &int_key;
                key_len = sizeof(unsigned long);
            }

            switch (op) {
            case OP_INSERT:
            case OP_UPDATE:
                val = get_val(&val_len);
                ret = kvstore->kv_put(tcontext, key, key_len, val, val_len);
                assert(ret == 0);
                break;

            case OP_READ:
                ret = kvstore->kv_get(tcontext, key, key_len, valres, &val_len);
                assert(ret == 0);
                __asm__ volatile("" : : "r"(val_len) : "memory");
                break;

            case OP_SCAN:
                kvstore->kv_scan(tcontext, key, key_len, 100, values);
                break;

            default:
                assert(0);
                break;
            }
        }
    }
}

static const char *ycsb_load_stage_fun(struct kvstore *kvstore, void *tcontext, int id) {
    do_op(kvstore, tcontext, &load_dec, id);
    return "load";
}

static const char *ycsb_op_stage_fun(struct kvstore *kvstore, void *tcontext, int id) {
    do_op(kvstore, tcontext, &op_dec, id);
    return "op";
}

void run_ycsb(const char *kvlib, int str_key_, int str_val_) {
    const char *(*stage_func[])(struct kvstore *, void *, int) = { ycsb_load_stage_fun, ycsb_op_stage_fun };
    struct kvstore kvstore;
    const char *engine;
    void *conf = NULL;
    str_key = str_key_;
    str_val = str_val_;
    ycsb_decompressor_init(&load_dec, LOAD_PATH, str_key);
    ycsb_decompressor_init(&op_dec, OP_PATH, str_key);
    load_kvstore(&kvstore, kvlib);
    engine = kvstore.kv_engine();
    if (!strcmp(engine, "bonsai")) {
        struct bonsai_config *c = malloc(sizeof(*c));
        c->stm_support = 0;
        conf = c;
    } else if (!strcmp(engine, "pacman")) {
        struct pacman_config *c = malloc(sizeof(*c));
        conf = c;
    } else if (!strcmp(engine, "lbtree")) {
        struct lbtree_config *c = malloc(sizeof(*c));
        conf = c;
    }
    run_kvstore(&kvstore, conf, 2, stage_func);
    free(conf);
}
