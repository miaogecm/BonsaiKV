#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include <unistd.h>

using namespace leveldb;

std::string db_mem = "/tmp/slmdb_transaction_example";
std::string db_disk = "/tmp/slmdb_transaction_example";

struct novelsm_config {
    int txn_support;
};

struct tctx {
    DB *db;
    WriteOptions write_options;
    ReadOptions read_options;
};

extern "C" {

const char *kv_engine() {
    return "novelsm";
}

void *kv_create_context(void *config) {
    auto *cfg = static_cast<novelsm_config *>(config);

    Options options;
    options.env = leveldb::Env::Default();
    options.create_if_missing = true;
    options.block_cache = nullptr;
    options.write_buffer_size = 0;
    options.block_size = 0;
    options.max_open_files = 0;
    options.filter_policy = NewBloomFilterPolicy(-1);
    options.reuse_logs = false;

    DB *db;
    Status s;
    if (cfg && cfg->txn_support) {
        assert(0);
    } else {
        s = DB::Open(options, db_disk, db_mem, &db);
    }
    assert(s.ok());

    return db;
}

void kv_destroy_context(void *context) {
    auto *db = static_cast<DB *>(context);
    delete db;
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
}

void *kv_thread_create_context(void *context, int id) {
    auto *ctx = new tctx;
    ctx->db = static_cast<DB *>(context);
    return ctx;
}

void kv_thread_destroy_context(void *tcontext) {
    auto *ctx = static_cast<tctx *>(tcontext);
    delete ctx;
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

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *ctx = static_cast<tctx *>(tcontext);
    ctx->db->Put(ctx->write_options, Slice((char *) key, key_len), Slice((char *) val, val_len));
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    auto *ctx = static_cast<tctx *>(tcontext);
    ctx->db->Delete(ctx->write_options, Slice((char *) key, key_len));
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *ctx = static_cast<tctx *>(tcontext);
    std::string val_;
    ctx->db->Get(ctx->read_options, Slice((char *) key, key_len), &val_);
    *val_len = val_.size();
    std::copy(val_.begin(), val_.end(), (char *) val);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    auto *ctx = static_cast<tctx *>(tcontext);
    auto *iter = ctx->db->NewIterator(ctx->read_options);
    std::vector<std::string> vals;
    for (iter->Seek(Slice((char *) key, key_len)); iter->Valid(); iter->Next()) {
        vals.push_back(iter->value().ToString());
    }
}

}