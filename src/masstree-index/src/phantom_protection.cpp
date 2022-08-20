#include <algorithm>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#define GLOBAL_VALUE_DEFINE
#include "masstree_wrapper.hpp"

using KeyType = uint64_t;
using ValueType = uint64_t;
using MT = MasstreeWrapper<ValueType>;
using NodeInfo = MT::node_info_t;
using NodeMap =
    std::unordered_map<const MT::node_type *,
                       uint64_t>; // key: node pointer, value: version

alignas(64) KeyType max_key = 3000;
KeyType insert_key = 0;
KeyType update_key = 0;
KeyType remove_key = 0;
ValueType global_value =
    97430172430124; // random global value stored in masstree

const ValueType *get_value_with_nodeinfo(MT *mt, KeyType key,
                                         NodeInfo &node_info) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->get_value_and_get_nodeinfo_on_failure(
      reinterpret_cast<char *>(&key_buf), sizeof(key_buf), node_info);
}

bool insert_value_with_nodeinfo(MT *mt, KeyType key, ValueType *value,
                                NodeInfo &node_info) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->insert_value_and_get_nodeinfo_on_success(
      reinterpret_cast<char *>(&key_buf), sizeof(key_buf), value, node_info);
}

bool update_value_with_nodeinfo(MT *mt, KeyType key, ValueType *value,
                                NodeInfo &node_info) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->update_value_and_get_nodeinfo_on_failure(
      reinterpret_cast<char *>(&key_buf), sizeof(key_buf), value, node_info);
}

bool remove_value_with_nodeinfo(MT *mt, KeyType key, NodeInfo &node_info) {
  KeyType key_buf{__builtin_bswap64(key)};
  return mt->remove_value_and_get_nodeinfo_on_failure(
      reinterpret_cast<char *>(&key_buf), sizeof(key_buf), node_info);
}

void scan_values_with_nodeinfo(MT *mt, KeyType l_key, KeyType r_key,
                               int64_t count, NodeMap &node_map) {
  KeyType l_key_buf{__builtin_bswap64(l_key)};
  KeyType r_key_buf{__builtin_bswap64(r_key)};

  bool l_exclusive = false;
  bool r_exclusive = false;

  uint64_t n_cnt = 0;
  uint64_t v_cnt = 0;

  bool exception_caught = false;

  mt->scan(
      reinterpret_cast<char *>(&l_key_buf), sizeof(l_key_buf), l_exclusive,
      reinterpret_cast<char *>(&r_key_buf), sizeof(r_key_buf), r_exclusive,
      {[&n_cnt, &node_map, &exception_caught](
           const MT::leaf_type *leaf, uint64_t version, bool &continue_flag) {
         auto it = node_map.find(leaf);
         if (it == node_map.end())
           node_map.emplace_hint(it, leaf, version);
         else if (it->second != version) {
           exception_caught = true;
           continue_flag = false;
         }

         n_cnt++;
         return;
       },
       [&v_cnt](const MT::Str &key, const ValueType *val, bool &continue_flag) {
         v_cnt++;
         // KeyType actual_key{__builtin_bswap64(*(reinterpret_cast<const
         // uint64_t *>(key.s)))}; printf("scanned key: %lu\n",
         // actual_key);
         (void)key;
         (void)val;
         (void)continue_flag;
         return;
       }},
      count);

  printf(exception_caught ? "ABORT\n" : "SUCCESS\n");
  printf("  scan n_cnt: %ld\n", n_cnt);
  printf("  scan v_cnt: %ld\n", v_cnt);
}

void rscan_values(MT *mt, KeyType l_key, KeyType r_key, int64_t count,
                  NodeMap &node_map) {
  KeyType l_key_buf{__builtin_bswap64(l_key)};
  KeyType r_key_buf{__builtin_bswap64(r_key)};

  bool l_exclusive = false;
  bool r_exclusive = false;

  uint64_t n_cnt = 0;
  uint64_t v_cnt = 0;

  bool exception_caught = false;
  mt->rscan(
      reinterpret_cast<char *>(&l_key_buf), sizeof(l_key_buf), l_exclusive,
      reinterpret_cast<char *>(&r_key_buf), sizeof(r_key_buf), r_exclusive,
      {[&n_cnt, &node_map, &exception_caught](
           const MT::leaf_type *leaf, uint64_t version, bool &continue_flag) {
         n_cnt++;
         auto it = node_map.find(leaf);
         if (it == node_map.end())
           node_map.emplace_hint(it, leaf, version);
         else if (it->second != version) {
           exception_caught = true;
           continue_flag = false;
         }
         return;
       },
       [&v_cnt](const MT::Str &key, const ValueType *val, bool &continue_flag) {
         v_cnt++;
         // KeyType actual_key{__builtin_bswap64(*(reinterpret_cast<const
         // uint64_t *>(key.s)))}; printf("rscanned key: %lu\n", actual_key);
         (void)key;
         (void)val;
         return;
       }},
      count);

  printf(exception_caught ? "ABORT\n" : "SUCCESS\n");
  printf("  rscan n_cnt: %ld\n", n_cnt);
  printf("  rscan v_cnt: %ld\n", v_cnt);
}

// insert keys except mid_key
void prepare_data(MT *mt) {
  KeyType l_key = 0;
  KeyType r_key = max_key;
  KeyType mid_key = (l_key + r_key) / 2;
  NodeInfo ni;
  for (KeyType k = l_key; k <= r_key; k++) {
    if (k == mid_key)
      continue;
    else {
      bool inserted = insert_value_with_nodeinfo(mt, k, &global_value, ni);
      always_assert(inserted, "failed to prepare data");
    }
  }
}

// read all the keys in range
void run_scan(MT *mt, NodeMap &nm) {
  KeyType l_key = 0;
  KeyType r_key = max_key;
  scan_values_with_nodeinfo(mt, l_key, r_key, -1, nm);
}

// insert mid_key (key in the middle of the range)
void run_insert_into_the_middle(MT *mt) {
  KeyType l_key = 0;
  KeyType r_key = max_key;
  KeyType mid_key = (l_key + r_key) / 2;
  NodeInfo ni;
  bool inserted = insert_value_with_nodeinfo(mt, mid_key, nullptr, ni);
  always_assert(inserted, "keys should all be unique");
}

void run_update_existing_key(MT *mt, ValueType *value) {
  KeyType l_key = 0; // update l_key
  NodeInfo ni;
  bool updated = update_value_with_nodeinfo(mt, l_key, value, ni);
  always_assert(updated, "failed update");
  const ValueType *updated_value = get_value_with_nodeinfo(mt, l_key, ni);
  if (value != updated_value) {
    printf("true pointer: %p, obtained pointer: %p\n", value, updated_value);
    printf("true value: %lu, obtained value: %lu\n", *value, *updated_value);
    always_assert(value == updated_value, "wrong content");
  }
}

void scan_insert_scan_test() {
  NodeMap nm;
  MT mt;
  mt.thread_init(0);
  printf("preparing data\n");
  prepare_data(&mt);
  printf("prepared data, running 1st scan\n");
  run_scan(&mt, nm);
  printf("ran 1st scan, inserting value into the middle node\n");
  run_insert_into_the_middle(&mt);
  printf("inserted, running 2nd scan\n");
  run_scan(&mt, nm); // Expected: ABORT
}

void scan_update_scan_test() {
  NodeMap nm;
  MT mt;
  mt.thread_init(0);
  printf("preparing data\n");
  prepare_data(&mt);
  printf("prepared data, running 1st scan\n");
  run_scan(&mt, nm);
  printf("ran 1st scan, updating value of existing key\n");
  auto temp = std::make_unique<ValueType>(2021);
  run_update_existing_key(&mt, temp.get());
  printf("updated and checked update, running 2nd scan\n");
  run_scan(&mt, nm); // Expected: SUCCESS
}

int main() {
  scan_insert_scan_test();
  // scan_update_scan_test();
}