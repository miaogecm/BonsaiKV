#include <iostream>
#include <string>
#include <filesystem>

#include "config.h"
#include "db.h"

extern "C" {

struct pacman_config {
    int num_workers;
};

const char *kv_engine() {
    return "pacman";
}

void *kv_create_context(void *config) {
    const size_t initial_size = 64 * 1024ul * 1024ul * 1024ul;  // 64 GiB
    auto *cfg = static_cast<pacman_config *>(config);
    auto *db = new DB("/mnt/ext4/dimm0/pacman", initial_size, cfg->num_workers, 0);
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
    auto *db = static_cast<DB *>(context);
    auto *client = new DB::Worker(db);
    return client;
}

void kv_thread_destroy_context(void *tcontext) {
    auto *client = static_cast<DB::Worker *>(tcontext);
    delete client;
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

static inline Slice get_key(void *key, size_t key_len) {
    return { (const char *) key, key_len };
}

static inline Slice get_val(void *val, size_t val_len) {
    return { (const char *) val, val_len };
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *client = static_cast<DB::Worker *>(tcontext);
    client->Put(get_key(key, key_len), get_val(val, val_len));
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    auto *client = static_cast<DB::Worker *>(tcontext);
    client->Delete(get_key(key, key_len));
    return 0;
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *client = static_cast<DB::Worker *>(tcontext);
    std::string val_;
    bool found = client->Get(get_key(key, key_len), &val_);
    assert(found);
    *val_len = val_.length();
    std::copy(val_.begin(), val_.end(), (char *) val);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    auto *client = static_cast<DB::Worker *>(tcontext);
    client->Scan(get_key(key, key_len), range);
}

}
