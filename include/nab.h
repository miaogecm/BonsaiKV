/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Numa-Aware Block Layer with PSO memory consistency model
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

#ifndef BONSAI_NAB_H
#define BONSAI_NAB_H

#include <assert.h>
#include "bonsai.h"
#include "region.h"
#include "cpu.h"

/* Decorate the pointer which points to somewhere in @which numa node. */
#define __node(which)

extern struct bonsai_info* bonsai;

#define NAB_ARENA_SIZE  DATA_REGION_SIZE
#define NAB_BLK_SIZE    64

#define NAB_NR_MISS_MAX 4

struct nab_blk_descriptor {
    union {
        struct {
            union {
                /* Used in global block table, the owner of the block. */
                int owner;
                /* Used in local block table, count how many misses. */
                unsigned int nr_miss;
            };
            unsigned int version;
        };
        unsigned long descriptor;
    };
};

struct nab_blk_table {
    struct nab_blk_descriptor blk_descriptors[NAB_ARENA_SIZE / NAB_BLK_SIZE];
};

static struct nab_blk_table global_table;

static inline void *__nab_node_ptr(void *ptr, int to, int from) {
    struct data_layer *d_layer = DATA(bonsai);
    return d_layer->region[to].start + (ptr - d_layer->region[from].start);
}

static inline void *__nab_blk_of(void *ptr) {
    return PTR_ALIGN_DOWN(ptr, NAB_BLK_SIZE);
}

static inline size_t __nab_blk_nr(void __node(0) *ptr) {
    struct data_layer *d_layer = DATA(bonsai);
    return (size_t) (ptr - d_layer->region[0].start) / NAB_BLK_SIZE;
}

static inline struct nab_blk_descriptor *__nab_get_blk_local_desc(void __node(my) *ptr) {
    struct data_layer *d_layer = DATA(bonsai);
    int my_node = get_numa_node(__this->t_cpu);
    size_t blk_nr = __nab_blk_nr(ptr);
    return &d_layer->region[my_node].nab->blk_descriptors[blk_nr];
}

static inline struct nab_blk_descriptor *__nab_get_blk_global_desc(void __node(0) *ptr) {
    return &global_table.blk_descriptors[__nab_blk_nr(ptr)];
}

static inline void __nab_load(void *dst, void __node(0) *src, size_t size) {
    struct nab_blk_descriptor *global_desc_ptr, *local_desc_ptr;
    struct nab_blk_descriptor global_desc, local_desc;
    int my_node = get_numa_node(__this->t_cpu);
    void __node(owner) *owner_ptr;
    void __node(my) *my_ptr;
    void *load_ptr;

    my_ptr = __nab_node_ptr(src, my_node, 0);

    global_desc_ptr = __nab_get_blk_global_desc(src);
    local_desc_ptr = __nab_get_blk_local_desc(my_ptr);

    global_desc.descriptor = ACCESS_ONCE(global_desc_ptr->descriptor);
    local_desc.descriptor = ACCESS_ONCE(local_desc_ptr->descriptor);

    if (global_desc.owner == my_node || global_desc.version == local_desc.version) {
        load_ptr = my_ptr;
    } else {
        owner_ptr = __nab_node_ptr(src, global_desc.owner, 0);
        if (xadd(&local_desc_ptr->nr_miss, 1) == NAB_NR_MISS_MAX) {
            memcpy(__nab_blk_of(my_ptr), __nab_blk_of(owner_ptr), NAB_BLK_SIZE);

            local_desc.version = global_desc.version;
            local_desc.nr_miss = 0;
            local_desc_ptr->descriptor = local_desc.descriptor;
            barrier();

            load_ptr = my_ptr;
        } else {
            load_ptr = owner_ptr;
        }
    }

    switch (size) {
        case 1:
            *(uint8_t *) dst = *(volatile uint8_t *) load_ptr;
            break;

        case 2:
            *(uint16_t *) dst = *(volatile uint16_t *) load_ptr;
            break;

        case 4:
            *(uint32_t *) dst = *(volatile uint32_t *) load_ptr;
            break;

        case 8:
            *(uint64_t *) dst = *(volatile uint64_t *) load_ptr;
            break;

        case NAB_BLK_SIZE:
            memcpy(dst, src, NAB_BLK_SIZE);
            break;

        default:
            assert(0);
    }
}

static inline void __node(my) *__nab_my_ptr(void __node(0) *ptr) {
    return __nab_node_ptr(ptr, get_numa_node(__this->t_cpu), 0);
}

static inline void __node(0) *__nab_node0_ptr(void __node(my) *ptr) {
    return __nab_node_ptr(ptr, 0, get_numa_node(__this->t_cpu));
}

#define nab_my_ptr(ptr)        ((typeof(ptr)) __nab_my_ptr(ptr))
#define nab_node0_ptr(ptr)     ((typeof(ptr)) __nab_node0_ptr(ptr))

static inline void *nab_owner_ptr(void __node(my) *ptr) {
    void __node(0) *p = nab_node0_ptr(ptr);
    return __nab_node_ptr(p, __nab_get_blk_global_desc(p)->owner, 0);
}

/*
 * Make a local (initialized) region in supervisor of nab.
 */
void nab_init_region(void __node(my) *start, size_t size, int initialized);

/*
 * Pull a region from its owner to current node.
 * Ensure that no concurrent write in [start, start + size).
 */
void nab_pull_region(void __node(my) *start, size_t size);

/*
 * Commit the modification of a region to all the numa nodes.
 */
void nab_commit_region(void __node(my) *start, size_t size);

#define __nab_dereference(ptr, data) ({  \
    typeof(*(ptr)) (data);  \
    __nab_load(&(data), ptr, sizeof(data));   \
    (data);   \
})
#define nab_dereference(ptr) \
    __nab_dereference(ptr, __UNIQUE_ID(data))

#endif //BONSAI_NAB_H
