/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Per-NODE Object Allocator
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */


#ifndef BONSAI_PER_NODE_H
#define BONSAI_PER_NODE_H

#include <stdint.h>
#include <stdlib.h>

#include "numa_config.h"
#include "atomic.h"
#include "thread.h"
#include "cpu.h"

typedef struct {
    void *arenas[NUM_SOCKET];
    size_t arena_sz, obj_sz;
    const char *name;
    atomic_t used;
} per_node_arena_t;

void per_node_arena_create(per_node_arena_t *arena, const char *name, size_t arena_sz, size_t obj_sz);
void *per_node_obj_alloc_onnode(per_node_arena_t *arena, int node);
void per_node_obj_free(per_node_arena_t *arena, void *o);

static inline void *per_node_obj_alloc(per_node_arena_t *arena) {
    return per_node_obj_alloc_onnode(arena, get_numa_node(__this->t_cpu));
}

static inline int node_of(per_node_arena_t *arena, void *o) {
    int i;
    for (i = 0; i < NUM_SOCKET; i++) {
        if (o >= arena->arenas[i] && o < arena->arenas[i] + arena->arena_sz) {
            return i;
        }
    }
    return -1;
}

static inline void *node_ptr(per_node_arena_t *arena, void *o, int node) {
    int in_node = node_of(arena, o);
    if (unlikely(in_node == -1)) {
        return NULL;
    }
    return arena->arenas[node] + (o - arena->arenas[in_node]);
}

#define this_node(arena, o)     \
    ((typeof(o)) node_ptr((arena), (o), get_numa_node(__this->t_cpu)))

#define for_each_obj(arena, i, p, o) \
    for ((i) = 0; (p) = node_ptr((arena), (o), (i)), (i) < NUM_SOCKET; (i)++)

#endif //BONSAI_PER_NODE_H
