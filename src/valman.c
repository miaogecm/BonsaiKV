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
    size_t nr_val_max, size;
};

static struct vclass_desc vclass_descs[] = {
    [VCLASS_16B]  = { 1024, 16 },
    [VCLASS_32B]  = { 1024, 32 },
    [VCLASS_64B]  = { 1024, 64 },
    [VCLASS_128B] = { 1024, 128 },
};

struct cpu_vpool_hdr {
    __le64 free[NR_VCLASS];
} __packed;

struct vpool_hdr {
    struct cpu_vpool_hdr cpu_vpool_hdrs[NUM_CPU];
} __packed;

struct cpu_vpool {
    pval_t free[NR_VCLASS];
} __attribute__((aligned(CACHELINE_SIZE)));

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

static void create_vpool() {
    struct data_layer *d_layer = DATA(bonsai);

    size_t hdr_sz = ALIGN(sizeof(struct vpool_hdr), PAGE_SIZE), i;
    void *curr[NUM_DIMM_PER_SOCKET];
    int dimm_idx, dimm, cpu, vc;
    struct vpool_hdr *hdr;
    __le64 *last_next;

    for (vc = 0; vc < NR_VCLASS; vc++) {
        assert(vclass_descs[vc].nr_val_max % NUM_DIMM == 0);
    }

    hdr = d_layer->val_region[node_idx_to_dimm(0, 0)].d_start;

    for (dimm_idx = 0; dimm_idx < NUM_DIMM_PER_SOCKET; dimm_idx++) {
        dimm = node_idx_to_dimm(0, dimm_idx);
        curr[dimm_idx] = d_layer->val_region[dimm].d_start + hdr_sz;
    }

    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        for (vc = 0; vc < NR_VCLASS; vc++) {
            last_next = &hdr->cpu_vpool_hdrs[cpu].free[vc];

            dimm_idx = 0;
            for (i = 0; i < vclass_descs[vc].nr_val_max; i++) {
                dimm = node_idx_to_dimm(0, dimm_idx);

                *last_next = pval_make_nv(vc, dimm, curr[dimm_idx] - d_layer->val_region[dimm].d_start);
                bonsai_flush(last_next, sizeof(__le64), 0);

                last_next = curr[dimm_idx] + vclass_descs[vc].size;
                curr[dimm_idx] += vclass_descs[vc].size + sizeof(__le64);

                dimm_idx = (dimm_idx + 1) % NUM_DIMM_PER_SOCKET;
            }

            *last_next = PVAL_NULL;

            bonsai_print("vpool created for cpu %d vc %d, free pval: %llx\n",
                         cpu, vc, hdr->cpu_vpool_hdrs[cpu].free[vc]);
        }
    }

    persistent_barrier();
}

static struct vpool *get_vpool() {
    struct vpool *vpool = malloc(sizeof(*vpool));
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
        return DATA(bonsai)->val_region[desc.dimm].d_start + desc.off;
    } else {
        return (void *) desc.addr;
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

static inline pval_t pval_nv_to_local(pval_t pval) {
    return pval_nv_to_node(pval, cpu_to_node(__this->t_cpu));
}

static inline pval_t pval_next_free(pval_t pval) {
    union pval_desc desc = { .pval = pval };
    assert(desc.is_nv);
    return *(__le64 *) (pval_ptr(pval) + vclass_descs[desc.vclass].size);
}

pval_t valman_make_nv_cpu(pval_t val, int cpu) {
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
    memcpy(dst, src, size);
    bonsai_flush(dst, size, 1);
    vpool->free[desc.vclass] = pval_next_free(n);
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

void valman_persist_cpu(int cpu) {
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

pval_t bonsai_make_val(enum vclass vclass, void *val) {
    return pval_make_v(vclass, val);
}

#endif
