/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Value Management
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 		   Junru Shen, gnu_emacs@hhu.edu.cn
 */

#ifndef BONSAI_VALMAN_H
#define BONSAI_VALMAN_H

#include "thread.h"

enum vclass {
    VCLASS_16B = 0,
    NR_VCLASS
};

#ifdef STR_VAL

pval_t valman_make_nv_cpu(pval_t val, int cpu);
void valman_free_nv(pval_t victim);

pval_t valman_make_v(pval_t val);
pval_t valman_make_v_local(pval_t val);
void valman_free_v(pval_t victim);
void *valman_extract_v(size_t *size, pval_t val);

void valman_persist_val(pval_t val);
void valman_persist_alloca_cpu(int cpu);

void valman_pull(pval_t val);

size_t valman_vpool_dimm_size();

void valman_vpool_init();

int valman_pval_is_remote(pval_t pval);

#else

static inline pval_t valman_make_nv_cpu(pval_t val, int cpu) {
    return val;
}

static inline void valman_free_nv(pval_t victim) {}

static inline pval_t valman_make_v(pval_t val) {
    return val;
}

static inline pval_t valman_make_v_local(pval_t val) {
    return val;
}

static inline void valman_free_v(pval_t victim) {}

static inline void *valman_extract_v(size_t *size, pval_t val) {
    assert(0);
}

void valman_persist_val(pval_t val) {}

static inline void valman_persist_alloca_cpu(int cpu) {}

static inline void valman_pull(pval_t val) {}

#endif

static inline pval_t valman_make_nv(pval_t val) {
    return valman_make_nv_cpu(val, __this->t_cpu);
}

static inline void valman_persist_alloca() {
    valman_persist_alloca_cpu(__this->t_cpu);
}

extern pval_t bonsai_make_val(enum vclass vclass, void *val);

extern void bonsai_free_val(pval_t victim);

extern void *bonsai_extract_val(size_t *size, pval_t val);

#endif //BONSAI_VALMAN_H
