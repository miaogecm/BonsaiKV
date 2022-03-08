/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Numa-Aware Block Layer
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

#define _GNU_SOURCE
#include "nab.h"

struct nab_blk_table *global_table = NULL;

void nab_init_region(void __node(my) *start, size_t size, int initialized) {
    struct data_layer *d_layer = DATA(bonsai);

    unsigned int n = ALIGN(size, NAB_BLK_SIZE) / NAB_BLK_SIZE, i;
    size_t blk_nr = __nab_blk_nr(nab_node0_ptr(start));
    int my_node = get_numa_node(__this->t_cpu), node;
    struct nab_blk_descriptor *desc;

    if (unlikely(!global_table)) {
        global_table = malloc(sizeof(*global_table));
    }

    for (i = blk_nr; i < blk_nr + n; i++) {
        desc = &global_table->blk_descriptors[i];
        desc->owner = my_node;
        desc->version = initialized ? 1 : 0;

        for (node = 0; node < NUM_SOCKET; node++) {
            desc = &d_layer->region[node].nab->blk_descriptors[i];
            desc->nr_miss = 0;
            desc->version = 0;
        }
    }
}

void nab_pull_region(void __node(my) *start, size_t size) {
    size_t n = ALIGN(size, NAB_BLK_SIZE) / NAB_BLK_SIZE;
    int my_node = get_numa_node(__this->t_cpu);
    struct nab_blk_descriptor *global_desc, desc;
    struct nab_blk_descriptor *local_desc;
    void __node(owner) *src;
    void __node(my) *dst;

    dst = PTR_ALIGN_DOWN(start, NAB_BLK_SIZE);

    while (n--) {
        global_desc = __nab_get_blk_global_desc(nab_node0_ptr(dst));
        local_desc = __nab_get_blk_local_desc(dst);

        if (global_desc->owner != my_node && global_desc->version != local_desc->version) {
            src = __nab_node_ptr(dst, global_desc->owner, my_node);

            /* Wait concurrent memcpy to be done. */
            while (ACCESS_ONCE(local_desc->nr_miss) == NAB_NR_MISS_MAX) {
                cpu_relax();
            }

            memcpy(dst, src, NAB_BLK_SIZE);

            desc = *local_desc;
            desc.version = global_desc->version;
            desc.nr_miss = 0;
            local_desc->descriptor = desc.descriptor;
        }

        dst += NAB_BLK_SIZE;
    }
}

void nab_commit_region(void __node(my) *start, size_t size) {
    unsigned int n = ALIGN(size, NAB_BLK_SIZE) / NAB_BLK_SIZE, i;
    size_t blk_nr = __nab_blk_nr(nab_node0_ptr(start));
    int my_node = get_numa_node(__this->t_cpu);
    struct nab_blk_descriptor *desc, d;

    for (i = blk_nr; i < blk_nr + n; i++) {
        desc = &global_table->blk_descriptors[i];

        d = *desc;
        d.owner = my_node;
        d.version++;

        desc->descriptor = d.descriptor;
    }
}
