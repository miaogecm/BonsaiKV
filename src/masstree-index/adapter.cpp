#include <algorithm>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <unistd.h>

#define GLOBAL_VALUE_DEFINE
#include "masstree_wrapper.hpp"

using ValueType = void;
using MT = MasstreeWrapper<ValueType>;

static std::atomic<int> tid = 0;
static __thread bool done_init = false;

static inline void check_thread_init() {
    if (done_init) {
        return;
    }
    int my_tid = std::atomic_fetch_add(&tid, 1);
    done_init = true;
    MT::thread_init(my_tid);
}

extern "C" {

void *index_init() {
    return new MT;
}

void index_destory(void* index_struct) {
    delete static_cast<MT *>(index_struct);
}

int index_insert(void* index_struct, const void *key, size_t len, const void *value) {
    auto *mt = static_cast<MT *>(index_struct);
    check_thread_init();
    mt->insert_value(static_cast<const char *>(key), len, const_cast<void *>(value));
    return 0;
}

int index_update(void* index_struct, const void *key, size_t len, const void* value) {
    return index_insert(index_struct, key, len, value);
}

int index_remove(void* index_struct, const void *key, size_t len) {
    auto *mt = static_cast<MT *>(index_struct);
    check_thread_init();
    mt->remove_value(static_cast<const char *>(key), len);
    return 0;
}

void* index_lowerbound(void* index_struct, const void *key, size_t len, const void *actual_key) {
    auto *mt = static_cast<MT *>(index_struct);
    check_thread_init();
    const void *res = nullptr;
    mt->rscan("\0", 1, false, static_cast<const char *>(key), len, false, {
        [](const MT::leaf_type *leaf, uint64_t version, bool &continue_flag) {},
        [&res, &actual_key](const MT::Str &key, const ValueType *val, bool &continue_flag) {
            res = val;
            if (actual_key) {
                memcpy((char *) actual_key, key.s, key.len);
            }
        }
    }, 1);
    return const_cast<void *>(res);
}

int index_scan(void* index_struct, const void *min, const void *max) {
    assert(0);
}

}
