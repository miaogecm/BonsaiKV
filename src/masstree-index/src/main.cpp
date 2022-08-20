#include <algorithm>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#define GLOBAL_VALUE_DEFINE
#include "masstree_wrapper.hpp"

using KeyType = uint64_t;
using ValueType = uint64_t;
using MT = MasstreeWrapper<ValueType>;

alignas(64) KeyType max_key = 300;
KeyType insert_key = 0;
KeyType update_key = 0;
KeyType remove_key = 0;

const ValueType *get_value(MT *mt, KeyType key) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->get_value(reinterpret_cast<char *>(&key_buf), sizeof(key_buf));
}

bool insert_value(MT *mt, KeyType key, ValueType *value) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->insert_value(reinterpret_cast<char *>(&key_buf), sizeof(key_buf),
                          value);
}

bool update_value(MT *mt, KeyType key, ValueType *value) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->update_value(reinterpret_cast<char *>(&key_buf), sizeof(key_buf),
                          value);
}

bool remove_value(MT *mt, KeyType key) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->remove_value(reinterpret_cast<char *>(&key_buf), sizeof(key_buf));
}

void scan_values(MT *mt, KeyType l_key, KeyType r_key, int64_t count = -1) {
  KeyType l_key_buf{__builtin_bswap64(l_key)};
  KeyType r_key_buf{__builtin_bswap64(r_key)};

  bool l_exclusive = false;
  bool r_exclusive = false;

  uint64_t n_cnt = 0;
  uint64_t v_cnt = 0;

  mt->scan(
      reinterpret_cast<char *>(&l_key_buf), sizeof(l_key_buf), l_exclusive,
      reinterpret_cast<char *>(&r_key_buf), sizeof(r_key_buf), r_exclusive,
      {[&n_cnt](const MT::leaf_type *leaf, uint64_t version,
                bool &continue_flag) {
         n_cnt++;
         (void)leaf;
         (void)version;
         (void)continue_flag;
         return;
       },
       [&v_cnt](const MT::Str &key, const ValueType *val, bool &continue_flag) {
         v_cnt++;
         KeyType actual_key{
             __builtin_bswap64(*(reinterpret_cast<const uint64_t *>(key.s)))};
         printf("scanned key: %lu\n", actual_key);
         (void)val;
         (void)continue_flag;
         return;
       }},
      count);

  printf("n_cnt: %ld\n", n_cnt);
  printf("v_cnt: %ld\n", v_cnt);
}

void rscan_values(MT *mt, KeyType l_key, KeyType r_key, int64_t count = -1) {
  KeyType l_key_buf{__builtin_bswap64(l_key)};
  KeyType r_key_buf{__builtin_bswap64(r_key)};

  bool l_exclusive = false;
  bool r_exclusive = false;

  uint64_t n_cnt = 0;
  uint64_t v_cnt = 0;

  mt->rscan(
      reinterpret_cast<char *>(&l_key_buf), sizeof(l_key_buf), l_exclusive,
      reinterpret_cast<char *>(&r_key_buf), sizeof(r_key_buf), r_exclusive,
      {[&n_cnt](const MT::leaf_type *leaf, uint64_t version,
                bool &continue_flag) {
         n_cnt++;
         (void)leaf;
         (void)version;
         (void)continue_flag;
         return;
       },
       [&v_cnt](const MT::Str &key, const ValueType *val, bool &continue_flag) {
         v_cnt++;
         KeyType actual_key{
             __builtin_bswap64(*(reinterpret_cast<const uint64_t *>(key.s)))};
         printf("rscanned key: %lu\n", actual_key);
         (void)val;
         (void)continue_flag;
         return;
       }},
      count);

  printf("n_cnt: %ld\n", n_cnt);
  printf("v_cnt: %ld\n", v_cnt);
}

void run_insert(MT *mt) {
  KeyType current_key = 0;
  bool inserted = 0;
  while (true) {
    current_key = fetch_and_add(&insert_key, 1);
    if (current_key > max_key)
      break;
    printf("inserting: %lu\n", current_key);
    inserted = insert_value(mt, current_key, nullptr);
    always_assert(inserted, "keys should all be unique");
  }
}

void run_update(MT *mt) {
  KeyType current_key = 0;
  bool updated = 0;
  while (true) {
    current_key = fetch_and_add(&update_key, 1);
    if (current_key % 10000 == 0)
      printf("update key: %ld\n", current_key);
    if (current_key > max_key)
      break;
    updated = update_value(mt, current_key, nullptr);
    always_assert(updated, "key should exist");
  }
}

void run_remove(MT *mt) {
  KeyType current_key = 0;
  bool removed = 0;
  while (true) {
    current_key = fetch_and_add(&remove_key, 1);
    printf("remove key: %ld\n", current_key);
    if (current_key > max_key)
      break;
    removed = remove_value(mt, current_key);
    always_assert(removed, "key should exist");
  }
}

void run_scan(MT *mt) {
  KeyType l_key = 0;
  KeyType r_key = 100;
  scan_values(mt, l_key, r_key, 20);
}

void run_rscan(MT *mt) {
  KeyType l_key = 301;
  KeyType r_key = 500;
  rscan_values(mt, l_key, r_key, 20);
}

void test_thread(MT *mt, std::size_t thid) {
  mt->thread_init(thid);
  run_insert(mt);
  sleep(1);
  // run_rscan(mt);
  // run_update(mt);
  // sleep(1);
  run_remove(mt);
}

int main() {
  std::size_t n_cores = std::thread::hardware_concurrency();
  std::size_t n_threads = std::max(std::size_t{1}, n_cores);
  // std::size_t n_threads = 1;
  printf("n_threads: %ld\n", n_threads);

  MT mt;

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  for (size_t i = 0; i < n_threads; i++) {
    threads.emplace_back(test_thread, &mt, i);
  }

  for (std::size_t i = 0; i < n_threads; i++) {
    threads[i].join();
  }
}