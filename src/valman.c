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

#include <malloc.h>

#include "bonsai.h"
#include "valman.h"

#ifdef STR_VAL

#define PVAL_NULL           (-1ul)

/*
 * We have 2 kinds of pvals:
 * V:  The pval is stored in DRAM. @addr is the low 48 bits of the address of the pval.
 * NV: The pval is stored in NVM. The pval locates at offset @off in NVDIMM @dimm.
 *
 * The pval that users passed in via operations like @bonsai_insert, or is returned by
 * @bonsai_lookup, is V pval. Bonsai's internal persistent data structure uses NV pval.
 */
union pval_desc {
    struct {
        int is_nv                 : 1;
        enum vclass vclass        : 15;
        union {
            unsigned long addr    : 48;
            struct {
                int dimm          : 8;
                unsigned long off : 40;
            } __packed;
        } __packed;
    };
    pval_t pval;
} __packed;

struct vclass_desc {
    size_t nr_val_max_per_cpu, size;
};

static struct vclass_desc vclass_descs[] = {
    [VCLASS]  = { CPU_VAL_POOL_SIZE, VAL_LEN },
};

struct cpu_vpool_hdr {
    __le64 free[NR_VCLASS];
} __packed;

struct vpool_hdr {
    struct cpu_vpool_hdr cpu_vpool_hdrs[NUM_CPU];
} __packed;

struct cpu_vpool {
    pval_t free[NR_VCLASS];
} ____cacheline_aligned2;

struct vpool {
    struct cpu_vpool cpu_vpools[NUM_CPU];
    struct vpool_hdr *hdr;
};

static inline pval_t pval_make_v(enum vclass vclass, void *addr) {
    union pval_desc desc = { .is_nv = 0, .vclass = vclass, .addr = (unsigned long) addr };
    return desc.pval;
}

static inline pval_t pval_make_nv(enum vclass vclass, int dimm, unsigned long off) {
    union pval_desc desc = { .is_nv = 1, .vclass = vclass, .dimm = dimm, .off = off };
    return desc.pval;
}

static size_t get_dimm_size_on_socket() {
    size_t size = ALIGN(sizeof(struct vpool_hdr), PAGE_SIZE), val_size;
    int i;
    for (i = 0; i < NR_VCLASS; i++) {
        val_size = vclass_descs[i].size + sizeof(unsigned long);
        size += NUM_CPU_PER_DIMM * val_size * vclass_descs[i].nr_val_max_per_cpu;
    }
    return size;
}

static void create_vpool() {
    struct data_layer *d_layer = DATA(bonsai);

    size_t hdr_sz = ALIGN(sizeof(struct vpool_hdr), PAGE_SIZE), nr, dimm_size_on_socket = get_dimm_size_on_socket();
    int node, cpu_idx, dimm_idx, dimm, cpu, vc, i;
    struct vpool_hdr *hdr;
    __le64 *last_next;
    void *curr;

    hdr = d_layer->val_region[node_idx_to_dimm(0, 0)].d_start;

    for (node = 0; node < NUM_SOCKET; node++) {
        cpu_idx = 0;

        for (dimm_idx = 0; dimm_idx < NUM_DIMM_PER_SOCKET; dimm_idx++) {
            dimm = node_idx_to_dimm(node, dimm_idx);

            curr = d_layer->val_region[dimm].d_start + hdr_sz + node * dimm_size_on_socket;

            for (i = 0; i < NUM_CPU_PER_DIMM; i++, cpu_idx++) {
                cpu = node_idx_to_cpu(node, cpu_idx);

                for (vc = 0; vc < NR_VCLASS; vc++) {
                    last_next = &hdr->cpu_vpool_hdrs[cpu].free[vc];

                    for (nr = 0; nr < vclass_descs[vc].nr_val_max_per_cpu; nr++) {
                        *last_next = pval_make_nv(vc, dimm, curr - d_layer->val_region[dimm].d_start);
                        bonsai_flush(last_next, sizeof(__le64), 0);

                        last_next = curr + vclass_descs[vc].size;
                        curr += vclass_descs[vc].size + sizeof(__le64);
                    }

                    *last_next = PVAL_NULL;

                    bonsai_print("vpool created for cpu %d vc %d, free pval: %llx\n",
                                 cpu, vc, hdr->cpu_vpool_hdrs[cpu].free[vc]);
                }
            }
        }
    }

    persistent_barrier();
}

static struct vpool *get_vpool() {
    struct vpool *vpool = memalign(CACHE_LINE_PREFETCH_UNIT * L1_CACHE_BYTES, sizeof(*vpool));
    int cpu, vc;
    vpool->hdr = DATA(bonsai)->val_region[node_idx_to_dimm(0, 0)].d_start;
    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        for (vc = 0; vc < NR_VCLASS; vc++) {
            vpool->cpu_vpools[cpu].free[vc] = vpool->hdr->cpu_vpool_hdrs[cpu].free[vc];
        }
    }
    return vpool;
}

static inline void *pval_ptr(pval_t pval) {
    union pval_desc desc = { .pval = pval };
    if (desc.is_nv) {
        return (void*)(DATA(bonsai)->val_region[desc.dimm].d_start + desc.off);
    } else {
        return (void*) desc.addr;
    }
}

static inline pval_t pval_nv_to_node(pval_t pval, int node) {
    union pval_desc desc = { .pval = pval };
    assert(desc.is_nv);
    if (dimm_to_node(desc.dimm) != node) {
        desc.dimm = node_idx_to_dimm(node, dimm_to_idx(desc.dimm));
    }
    return desc.pval;
}

int valman_pval_is_remote(pval_t pval) {
    union pval_desc desc = { .pval = pval };
    assert(desc.is_nv);
    return dimm_to_node(desc.dimm) != cpu_to_node(__this->t_cpu);
}

static inline pval_t pval_nv_to_local(pval_t pval) {
#ifdef ENABLE_PNODE_REPLICA
    return pval_nv_to_node(pval, cpu_to_node(__this->t_cpu));
#else
    return pval;
#endif
}

static inline pval_t pval_next_free(pval_t pval) {
    union pval_desc desc = { .pval = pval };
    assert(desc.is_nv);
    return *(__le64 *) (pval_ptr(pval) + vclass_descs[desc.vclass].size);
}

pval_t valman_make_nv_cpu(pval_t val, int cpu) {
    pthread_mutex_t *dimm_lock = LOG(bonsai)->desc->descs[cpu].dimm_lock;
    struct cpu_vpool *vpool = &DATA(bonsai)->vpool->cpu_vpools[cpu];
    union pval_desc desc = { .pval = val };
    void *dst, *src;
    size_t size;
    pval_t n;
    n = pval_nv_to_node(vpool->free[desc.vclass], cpu_to_node(cpu));
    assert(n != PVAL_NULL);
    dst = pval_ptr(n);
    src = pval_ptr(val);
    size = vclass_descs[desc.vclass].size;
    pthread_mutex_lock(dimm_lock);
    memcpy(dst, src, size);
    vpool->free[desc.vclass] = pval_next_free(vpool->free[desc.vclass]);
    bonsai_flush(dst, size, 0);
    pthread_mutex_unlock(dimm_lock);
    return n;
}

void valman_free_nv(pval_t victim) {
    /* TODO: How to work with delay-persist? */
}

pval_t valman_make_v(pval_t val) {
    union pval_desc desc = { .pval = val };
    size_t size = vclass_descs[desc.vclass].size;
    void *buf = malloc(size);
    memcpy(buf, pval_ptr(val), size);
    return pval_make_v(desc.vclass, buf);
}

pval_t valman_make_v_local(pval_t val) {
    return valman_make_v(pval_nv_to_local(val));
}

void valman_free_v(pval_t victim) {
    free(pval_ptr(victim));
}

void *valman_extract_v(size_t *size, pval_t val) {
    union pval_desc desc = { .pval = val };
    assert(!desc.is_nv);
    *size = vclass_descs[desc.vclass].size;
    return pval_ptr(val);
}

void valman_persist_val(pval_t val) {
    union pval_desc desc = { .pval = val };
    size_t size = vclass_descs[desc.vclass].size;
    void *p = pval_ptr(val);
    bonsai_flush(p, size, 0);
}

void valman_persist_alloca_cpu(int cpu) {
    struct vpool *vpool = DATA(bonsai)->vpool;
    union pval_desc desc;
    int vc;
    for (vc = 0; vc < NR_VCLASS; vc++) {
        desc.pval = vpool->cpu_vpools[cpu].free[vc];
        assert(desc.is_nv);
        assert(desc.vclass == vc);
        vpool->hdr->cpu_vpool_hdrs[cpu].free[vc] = desc.pval;
        bonsai_flush(&vpool->hdr->cpu_vpool_hdrs[cpu].free[vc], sizeof(__le64), 0);
    }
    persistent_barrier();
}

void valman_pull(pval_t val) {
    union pval_desc desc = { .pval = val };
    pval_t local = pval_nv_to_local(val);
    if (local == val) {
        return;
    }
    memcpy(pval_ptr(local), pval_ptr(val), vclass_descs[desc.vclass].size);
}

size_t valman_vpool_dimm_size() {
    return get_dimm_size_on_socket() * NUM_SOCKET;
}

void valman_vpool_init() {
    create_vpool();
    DATA(bonsai)->vpool = get_vpool();
}

pval_t bonsai_make_val(enum vclass vclass, void *val) {
    return pval_make_v(vclass, val);
}

void bonsai_free_val(pval_t victim) {
    valman_free_v(victim);
}

void *bonsai_extract_val(size_t *size, pval_t val) {
    return valman_extract_v(size, val);
}

#endif
