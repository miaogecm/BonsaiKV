#pragma once

#include "db.h"
#include "index/masstree/masstree.h"

class MasstreeIndex : public Index {
 public:
  MasstreeIndex() { mt_ = masstree_create(nullptr); }

  virtual ~MasstreeIndex() override { masstree_destroy(mt_); }

  void MasstreeThreadInit(int thread_id) { }

  virtual ValueType Get(const Slice &key) override {
    ValueType val;
    uint64_t k = __builtin_bswap64(*(KeyType *)key.data());
    val = reinterpret_cast<ValueType>(masstree_get(mt_, &k, sizeof(k)));
    return val;
  }

  virtual void Put(const Slice &key, LogEntryHelper &le_helper) override {
    ValueType val = le_helper.new_val;
    uint64_t k = __builtin_bswap64(*(KeyType *)key.data());
    masstree_put(mt_, &k, sizeof(k), (void *) val);
  }

  virtual void GCMove(const Slice &key, LogEntryHelper &le_helper) override {
      assert(0);
  }

  virtual void Delete(const Slice &key) override {
    // TODO
  }

  virtual void Scan(const Slice &key, int cnt,
                    std::vector<ValueType> &vec) override {
      assert(0);
  }

  // virtual void PrefetchEntry(const Shortcut &sc) override {}

 private:
  masstree_t *mt_;

  DISALLOW_COPY_AND_ASSIGN(MasstreeIndex);
};
