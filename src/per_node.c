/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Per-NODE Object Allocator
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

#include <numa.h>
#include <assert.h>

#include "per_node.h"

void per_node_arena_create(per_node_arena_t *arena, const char *name, size_t arena_sz, size_t obj_sz) {
    int i;
    for (i = 0; i < NUM_SOCKET; i++) {
        arena->arenas[i] = numa_alloc_onnode(arena_sz, i);
    }
    arena->arena_sz = arena_sz;
    arena->obj_sz = obj_sz;
    arena->name = name;
    atomic_set(&arena->used, 0);
}

void *per_node_obj_alloc_onnode(per_node_arena_t *arena, int node) {
    int i = atomic_add_return(1, &arena->used) - 1;
    size_t off = i * arena->obj_sz;
    assert(off + arena->obj_sz <= arena->arena_sz);
    return arena->arenas[node] + off;
}

void per_node_obj_free(per_node_arena_t *arena, void *o) {
    /* TODO: per_node_obj_free */
}
