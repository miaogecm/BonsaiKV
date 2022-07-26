#include "leveldb/persistant_pool.h"

#include <libvmem.h>

namespace leveldb {
namespace nvram {

#define LAYOUT_NAME "PMINDEXDB"

VMEM *pm_pool;
static bool init = false;
static uint64_t allocs = 0;


void create_pool(const std::string& dir, const size_t& s) {
  size_t size = (s < VMEM_MIN_POOL) ? VMEM_MIN_POOL : s;
  printf("Creating NVM pool size of %lu\n", size);
  pm_pool = vmem_create(dir.data(), size);
  init = true;
  if (pm_pool == nullptr) {
    fprintf(stderr, "pmem create error\n");
    perror(dir.data());
    exit(1);
  }
}

void close_pool() {
  if (init) {
    fprintf(stdout, "pmem allocs %lu\n", allocs);
    //pmemcto_close(pm_pool);
  }
}

void pfree(void* ptr) {
  if (!init) {
    free(ptr);
  } else {
    vmem_free(pm_pool, ptr);
  }
}

void* pmalloc(size_t size) {
  void* ptr;
  if (!init) {
    ptr = malloc(size);
  } else {
    allocs++;
    if ((ptr = vmem_malloc(pm_pool, size)) == nullptr) {
      fprintf(stderr, "pmem malloc error 2 \n");
      perror("vmem_malloc");
      exit(1);
    }
  }
  return ptr;
}

void stats() {
//  char *msg;
//  pmemcto_stats_print(vmem, msg);
//  printf("%s\n", msg);
}

}
}