/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Numa-Aware Block Layer with PSO memory consistency model
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

#include "nab.h"

void nab_init_region(void __node(my) *start, size_t size, int initialized) {
    struct data_layer *d_layer = DATA(bonsai);

    unsigned int n = ALIGN(size, NAB_BLK_SIZE) / NAB_BLK_SIZE, i;
    int my_node = get_numa_node(__this->t_cpu), node;
    size_t blk_nr = __nab_blk_nr(start);
    struct nab_blk_descriptor *desc;

    for (i = blk_nr; i < blk_nr + n; i++) {
        desc = &global_table.blk_descriptors[i];
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
    struct nab_blk_descriptor *desc;
    void __node(owner) *src;
    void __node(my) *dst;

    dst = PTR_ALIGN_DOWN(start, NAB_BLK_SIZE);
    while (n--) {
        desc = __nab_get_blk_global_desc(start);
        if (desc->owner != my_node) {
            src = __nab_node_ptr(start, desc->owner, 0);
            /* Wait concurrent memcpy to be done. */
            while (ACCESS_ONCE(__nab_get_blk_local_desc(dst)->nr_miss) == NAB_NR_MISS_MAX) {
                cpu_relax();
            }
            memcpy(dst, src, NAB_BLK_SIZE);
        }
        dst += NAB_BLK_SIZE;
    }
}

void nab_commit_region(void __node(my) *start, size_t size) {
    unsigned int n = ALIGN(size, NAB_BLK_SIZE) / NAB_BLK_SIZE, i;
    int my_node = get_numa_node(__this->t_cpu);
    size_t blk_nr = __nab_blk_nr(start);
    struct nab_blk_descriptor *desc, d;

    for (i = blk_nr; i < blk_nr + n; i++) {
        desc = &global_table.blk_descriptors[i];

        d = *desc;
        d.owner = my_node;
        d.version++;

        desc->descriptor = d.descriptor;
    }
}
