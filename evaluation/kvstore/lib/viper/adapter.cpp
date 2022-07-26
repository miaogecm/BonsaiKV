#include "viper/viper.hpp"

#define STRING_KEY
#define STRING_VAL

#ifdef STRING_KEY
typedef std::string Key;
#else
typedef uint64_t Key;
#endif

#ifdef STRING_VAL
typedef std::string Val;
#else
typedef uint64_t Val;
#endif

typedef viper::Viper<Key, Val> DB;
typedef DB::Client             DBClient;

extern "C" {

const char *kv_engine() {
    return "viper";
}

void *kv_create_context(void *config) {
    const size_t initial_size = 1073741824;  // 1 GiB
    auto *db = DB::create_new("/mnt/pmem2/viper", initial_size);
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
    auto *client = new DBClient(db->get_client());
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
#ifdef STRING_KEY
    return { (const char *) key, key_len };
#else
    assert(key_len == sizeof(unsigned long));
    return __builtin_bswap64(*(unsigned long *) key);
#endif
}

static inline Val get_val(void *val, size_t val_len) {
#ifdef STRING_VAL
    return { (const char *) val, val_len };
#else
    assert(val_len == sizeof(unsigned long));
    return *(unsigned long *) val;
#endif
}

int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len) {
    auto *client = static_cast<DBClient *>(tcontext);
    return client->put(get_key(key, key_len), get_val(val, val_len));
}

int kv_del(void *tcontext, void *key, size_t key_len) {
    auto *client = static_cast<DBClient *>(tcontext);
    return client->remove(get_key(key, key_len));
}

int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len) {
    auto *client = static_cast<DBClient *>(tcontext);
    std::string val_;
    bool found = client->get(get_key(key, key_len), &val_);
    assert(found);
    *val_len = val_.length();
    std::copy(val_.begin(), val_.end(), (char *) val);
    return 0;
}

void kv_scan(void *tcontext, void *key, size_t key_len, int range, void *values) {
    assert(0);
}

}
