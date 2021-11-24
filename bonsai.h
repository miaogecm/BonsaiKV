#ifndef _BONSAI_H
#define _BONSAI_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <libpmemobj.h>

#include "atomic.h"
#include "commen.h"
#include "index_layer/skiplist.h"
#include "cuckoo_hash.h"

typedef skiplist_t index_layer_t;

#define NODE_SIZE       32
#define BUFFER_SIZE_MUL 2
#define NUMA_COPY_SIZE  8

typedef cuckoo_hash_t buffer_node_t;

typedef struct persist_node_s {
    uint64_t bitmap;
    uint64_t slot[NODE_SIZE+1];
    entry_t entry[NODE_SIZE];
    rwlock_t* slot_lock;
    struct persist_node_s* prev;
    struct persist_node_s* next;
    buffer_node_t* buffer_node;
}persist_node_t;

typedef struct persist_layer_s {
    persist_node_t* head;
    persist_node_t* tail;
}persist_layer_t;

typedef struct op_log_s {
    atomic_t time_stamp;
    persist_node_t* dst_node;
    entry_t entry;
}op_log_t;

typedef struct log_list_s {
    op_log_t* head;
    op_log_t* tail;
}log_list_t;

#define CPU_MAX_NUM     48

typedef struct buffer_layer_s {
    log_list_t log_list[CPU_MAX_NUM];
}buffer_layer_t;

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, persist_node_t);
POBJ_LAYOUT_END(BONSAI);

typedef struct bonsai_s {
    uint8_t init;
    index_layer_t* index_layor;
    buffer_layer_t* buffer_layer;
    persist_layer_t* persist_layer;
}bonsai_t;

bonsai_t* bonsai_init();
int bonsai_lookup(bonsai_t* bonsai, entry_key_t key, char* result);
int bonsai_insert(bonsai_t* bonsai, entry_kry_t key, char* value);
int bonsai_remove(bonsai_t* bonsai, entry_key_t key);
int bonsai_range(bonsai_t* bonsai, entry_key_t begin_key, entry_key_t end_t, cahr** res_arr);

#define index_layer_init()              sl_init()
#define index_layer_insert(a, b, c)     sl_insert(a, b, c)
#define index_layer_remove(a, b)        sl_remove(a, b)
#define index_layer_lower_bound(a, b)   sl_lower_bound(a, b)

#define INDEX(bonsai)    (bonsai->index_layer)
#define BUFFER(bonsai)   (bonsai->buffer_layer)
#define PERSIST(bonsai)  (bonsai->persist_layer)

#define NUMA_NODE_NUM       2
#define POBJ_POOL_SIZE		(10 * 1024ULL * 1024ULL * 1024ULL) // 10 GB
static char* dax_file[NUMA_NODE_NUM] = {"/mnt/ext4/pool1",
                                        "/mnt/ext4/pool2"};
PMEMobjpool* pop[NUMA_NODE_NUM];

#define CACHELINE_SIZE      64

#define _mm_clflushopt(addr)\
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char*)(addr)))


static inline void clflush(void* buf, uint32_t len) {
    int i;
    len = len + ((unsigned long)(buf) & (CACHELINE_SIZE - 1));
    for (i = 0; i < len; i += CACHELINE_SIZE)
        _mm_clflushopt(buf + i);    
}

buffer_node_t* buffer_layer_init() {
    buffer_node_t* b_layer = (buffer_node_t*) malloc(sizeof(buffer_layer_t));
    assert(b_layer);
    int i;
    for (i = 0; i < CPU_MAX_NUM; i++) {
        b_layer->log_list[i].head = (op_log_t*) malloc(sizeof(op_log_t));
        assert(b_layer->log_list[i].head);
        b_layer->log_list[i].tail = b_layer->log_list[i].head;
        b_layer->log_list[i].head->next = NULL;
    }
    return b_layer;
}

bonsai_t* get_bosai() {
    PMEMoid root = pmemobj_root(pop[0], sizeof(bonsai_t));
    return pmemobj_direct(root);
}

persist_node_t* alloc_persist_node() {
    TOID(persist_node_t) toid;
    POBJ_ZALLOC(pop[0], &toid, persist_node_t, sizeof(persist_node_t));
    D_RW(toid)->slot_lock = rwlock_init();
    D_RW(toid)->buffer_node = cuckoo_init(BUFFER_SIZE_MUL * NODE_SIZE);
    return pmemobj_direct(toid);
}

persist_layer_t* persist_layer_init() {
    p_layer->tail = alloc_persist_node();
    p_layer->tail
    
    p_layer->head->bitmap = 1;

}

bonsai_t* bonsai_init() {
    bonsai_t* bonsai = get_bonsai();
    if (!bonsai->init) {
        INDEX(bonsai) = sl_init();
        BUFFER(bonsai) = buffer_layer_init();
        PERSIST(bonsai) = persist_layer_init();
    } else {
        bonsai_recover(bonsai);
    }
    return bonsai;
}

#endif
/*bonsai.h*/