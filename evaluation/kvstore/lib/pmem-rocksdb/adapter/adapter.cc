#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/transaction_db.h>
#include <unistd.h>

using namespace ROCKSDB_NAMESPACE;

std::string db_path = "/mnt/ext4/dimm0/rocksdb";

struct rocksdb_config {
    int txn_support;
};

struct tctx {
    union {
        DB *db;
        TransactionDB *tdb;
    };
    Transaction *txn;
    WriteOptions write_options;
    ReadOptions read_options;
};

extern "C" {

const char *kv_engine() {
    return "rocksdb";
}

void *kv_create_context(void *config) {
    auto *cfg = static_cast<rocksdb_config *>(config);

    Options options;
    options.IncreaseParallelism((int) sysconf(_SC_NPROCESSORS_ONLN));
    options.OptimizeLevelStyleCompaction();
    options.env = NewDCPMMEnv(DCPMMEnvOptions());
    options.create_if_missing = true;
    options.recycle_dcpmm_sst = true;
    options.allow_dcpmm_writes = true;

    DB *db;
    Status s;
    if (cfg->txn_support) {
        TransactionDB *tdb;
        TransactionDBOptions txn_db_options;
        s = TransactionDB::Open(options, txn_db_options, db_path, &tdb);
        db = tdb;
    } else {
        s = DB::Open(options, db_path, &db);
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
    ctx->txn = nullptr;
    return ctx;
}

void kv_thread_destroy_context(void *tcontext) {
    auto *ctx = static_cast<tctx *>(tcontext);
    assert(!ctx->txn);
    delete ctx;
}

void kv_thread_start_test(void *tcontext) {
}

void kv_thread_stop_test(void *tcontext) {
}

void kv_txn_begin(void *tcontext) {
    auto *ctx = static_cast<tctx *>(tcontext);
    ctx->txn = ctx->tdb->BeginTransaction(ctx->write_options);
}

void kv_txn_commit(void *tcontext) {
    auto *ctx = static_cast<tctx *>(tcontext);
    ctx->txn->Commit();
}

void kv_txn_rollback(void *tcontext) {
    auto *ctx = static_cast<tctx *>(tcontext);
    ctx->txn->Rollback();
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