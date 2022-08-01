#ifndef SRC_CNA_H
#define SRC_CNA_H

typedef struct cna_node {
    _Atomic(uintptr_t) spin;
    _Atomic(int) socket;
    _Atomic(struct cna_node *) secTail;
    _Atomic(struct cna_node *) next;
} cna_node_t;

typedef struct {
    _Atomic(cna_node_t *) tail;
} cna_lock_t;

void cna_lock_init(cna_lock_t *lock);
void cna_lock(cna_lock_t *lock);
void cna_unlock(cna_lock_t *lock);

#endif //SRC_CNA_H
