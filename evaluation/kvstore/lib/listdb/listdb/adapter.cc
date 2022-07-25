#include "listdb.h"
#include "db_client.h"

extern "C" {

const char *kv_engine() {
    return "listdb";
}

void *kv_create_context(void *config) {
    auto *db = new ListDB();
    return db;
}

void kv_destroy_context(void *context) {
    auto *db = static_cast<ListDB *>(context);
    delete db;
}

void kv_start_test(void *context) {
}

void kv_stop_test(void *context) {
}

void *kv_thread_create_context(void *context, int id) {
    auto *db = static_cast<ListDB *>(context);
    auto *client = new DBClient(db, id, GetChip());
    return client;
}

void kv_thread_destroy_context(void *tcontext) {
    auto *client = static_cast<DBClient *>(tcontext);
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

static inline Key get_key(void *key, size_t key_len) {
#ifdef LISTDB_STRING_KEY
    char key_[kStringKeyLength] = {0};
    memcpy(key_, key, key_len);
    return Key(key_);
#else
    assert(key_len == sizeof(unsigned long));
    return __builtin_bswap64(*(unsigned long *) key);
#endif
}

static inline Value get_val(void *val, size_t val_len) {
#ifdef LISTDB_WISCKEY
    assert(0);
#else
    assert(val_len == sizeof(unsigned long));
    return *(unsigned long *) val;
#endif
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *client = static_cast<DBClient *>(tcontext);
    Key key_ = get_key(key, key_len);
#ifdef LISTDB_WISCKEY
    client->PutStringKV(key_.data(), std::string_view(static_cast<const char *>(val), val_len));
#else
    client->Put(key_, get_val(val, val_len));
#endif
    return 0;
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    assert(0);
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *client = static_cast<DBClient *>(tcontext);
    Key key_ = get_key(key, key_len);
    Value val_;
#ifdef LISTDB_WISCKEY
    bool found = client->GetStringKV(key_.data(), &val_);
    assert(found);
    PmemPtr value_paddr(val_);
    char *value_buf = (char *) value_paddr.get();
    std::string_view value_sv(value_buf + 8, *((size_t *) value_buf));
    memcpy(val, value_sv.data(), value_sv.size());
    *val_len = value_sv.size();
    return 0;
#else
    bool found = client->Get(key_, &val_);
    assert(found);
    *(unsigned long *) val = val_;
    *val_len = 8;
    return 0;
#endif
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    assert(0);
}

}
