#ifndef PMEM_H
#define PMEM_H

#define POBJ_POOL_SIZE		(10 * 1024ULL * 1024ULL * 1024ULL) // 10 GB
static char* dax_file[NUMA_NODE_NUM] = {"/mnt/ext4/pool1",
                                        "/mnt/ext4/pool2"};
PMEMobjpool* pop[NUMA_NODE_NUM];

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, persist_node_t);
POBJ_LAYOUT_END(BONSAI);

#endif
