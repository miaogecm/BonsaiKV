/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "pnode.h"
#include "oplog.h"
#include "bonsai.h"
#include "arch.h"
#include "bitmap.h"

#define PNODE_FANOUT            40
#define PNODE_INTERLEAVING_SIZE 256

#define PNODE_NUM_ENT_BLK       (PNODE_FANOUT * sizeof(pnoent_t) / PNODE_INTERLEAVING_SIZE)
#define PNODE_NUM_BLK           (PNODE_NUM_ENT_BLK + 1)

#define NOT_FOUND               (-1u)

static unsigned global_epoch;

struct oplog;

typedef struct pnoent {
    pkey_t      k;
    __le64      v : 48;
    uint16_t    epoch;
} __attribute__((packed)) pnoent_t;

typedef struct mnode {
    /* cacheline 0 */
    __le64       validmap;
    __le8        fgprt[PNODE_FANOUT];
    char         padding[16];

    /* cacheline 1 */
    pnoid_t      prev, next;
    /* [lfence, rfence) */
    pkey_t       lfence, rfence;
} mnode_t;

typedef struct dnode {
    pnoent_t     ents[PNODE_INTERLEAVING_SIZE / sizeof(pnoent_t)];
} dnode_t;

#define PNODE_NUM_PERMUTE       64

int pnode_dimm_permute[PNODE_NUM_PERMUTE][PNODE_NUM_BLK] = {
    { 5, 3, 2, 1, 4, 0 }, 
    { 5, 0, 2, 3, 1, 4 }, 
    { 1, 3, 2, 5, 4, 0 }, 
    { 2, 4, 3, 5, 0, 1 }, 
    { 2, 3, 1, 5, 4, 0 }, 
    { 4, 3, 0, 2, 1, 5 }, 
    { 2, 1, 3, 0, 4, 5 }, 
    { 4, 1, 5, 2, 0, 3 }, 
    { 4, 2, 0, 5, 3, 1 }, 
    { 3, 1, 4, 2, 0, 5 }, 
    { 0, 1, 3, 4, 2, 5 }, 
    { 5, 4, 1, 3, 0, 2 }, 
    { 3, 5, 1, 0, 4, 2 }, 
    { 0, 3, 5, 2, 4, 1 }, 
    { 3, 2, 0, 5, 1, 4 }, 
    { 1, 3, 2, 4, 5, 0 }, 
    { 2, 4, 0, 1, 3, 5 }, 
    { 5, 1, 3, 2, 0, 4 }, 
    { 5, 0, 4, 2, 1, 3 }, 
    { 1, 4, 2, 5, 0, 3 }, 
    { 2, 0, 4, 3, 5, 1 }, 
    { 2, 5, 4, 0, 3, 1 }, 
    { 2, 1, 0, 4, 3, 5 }, 
    { 0, 5, 1, 2, 3, 4 }, 
    { 5, 2, 4, 0, 1, 3 }, 
    { 4, 2, 5, 1, 3, 0 }, 
    { 0, 3, 5, 4, 2, 1 }, 
    { 3, 2, 1, 4, 5, 0 }, 
    { 3, 2, 4, 0, 1, 5 }, 
    { 0, 3, 5, 1, 2, 4 }, 
    { 2, 4, 5, 0, 3, 1 }, 
    { 5, 1, 2, 4, 3, 0 }, 
    { 2, 4, 3, 0, 1, 5 }, 
    { 2, 5, 4, 3, 1, 0 }, 
    { 3, 4, 1, 5, 0, 2 }, 
    { 4, 0, 2, 3, 1, 5 }, 
    { 3, 4, 5, 1, 0, 2 }, 
    { 4, 1, 2, 3, 5, 0 }, 
    { 1, 3, 0, 5, 4, 2 }, 
    { 3, 5, 1, 0, 4, 2 }, 
    { 4, 0, 5, 1, 2, 3 }, 
    { 4, 0, 3, 2, 5, 1 }, 
    { 3, 1, 4, 2, 0, 5 }, 
    { 0, 1, 2, 4, 5, 3 }, 
    { 5, 3, 1, 4, 0, 2 }, 
    { 1, 5, 4, 0, 2, 3 }, 
    { 0, 4, 1, 2, 3, 5 }, 
    { 0, 1, 2, 4, 3, 5 }, 
    { 1, 5, 0, 4, 2, 3 }, 
    { 5, 4, 2, 1, 0, 3 }, 
    { 1, 5, 3, 0, 2, 4 }, 
    { 2, 5, 1, 0, 3, 4 }, 
    { 5, 1, 3, 2, 4, 0 }, 
    { 2, 5, 1, 3, 0, 4 }, 
    { 1, 3, 5, 0, 2, 4 }, 
    { 4, 2, 5, 1, 3, 0 }, 
    { 0, 2, 5, 3, 1, 4 }, 
    { 4, 2, 1, 3, 5, 0 }, 
    { 1, 5, 2, 4, 0, 3 }, 
    { 1, 4, 3, 0, 5, 2 }, 
    { 1, 0, 4, 5, 2, 3 }, 
    { 3, 0, 4, 1, 2, 5 }, 
    { 0, 2, 4, 5, 1, 3 }, 
    { 0, 5, 3, 1, 2, 4 }, 
};

union pnoid_u {
    struct {
        uint32_t node : 3;
        uint32_t off  : 29;
    };
    pnoid_t id;
};

int pnode_node(pnoid_t pno) {
    union pnoid_u u = { .id = pno };
    return (int) u.node;
}

static inline unsigned long pnode_off(pnoid_t pno) {
    union pnoid_u u = { .id = pno };
    return u.off * (unsigned long) PNODE_INTERLEAVING_SIZE;
}

static inline int *pnode_permute(pnoid_t pno) {
    return pnode_dimm_permute[pno % PNODE_NUM_PERMUTE];
}

static inline void *pnode_dimm_addr(pnoid_t pno, int dimm_idx) {
    int node = pnode_node(pno), dimm = node_to_dimm(node, dimm_idx);
    return (void *) DATA(bonsai)->region[dimm].start + pnode_off(pno);
}

static inline void *pnode_get_blk(pnoid_t pno, unsigned blk) {
    return pnode_dimm_addr(pno, pnode_permute(pno)[blk]);
}

static mnode_t *pnode_meta(pnoid_t pno) {
    return pnode_get_blk(pno, 0);
}

static inline void *pnoptr_cvt_node(void *ptr, int to_node, int from_node) {
    struct data_layer *d_layer = DATA(bonsai);
    int dimm_idx, dimm;
    void *start;
    for (dimm_idx = 0; dimm_idx < NUM_DIMM_PER_SOCKET; dimm_idx++) {
        dimm = node_to_dimm(from_node, dimm_idx);
        start = (void *) d_layer->region[dimm].start;
        if (ptr >= start && ptr < start + DATA_REGION_SIZE) {
            dimm = node_to_dimm(to_node, dimm_idx);
            return (void *) d_layer->region[dimm].start + (ptr - start);
        }
    }
    assert(0);
}

static inline pnoent_t *ent_fetch(pnoent_t *ent, int node) {
    int my = get_numa_node(__this->t_cpu);
    pnoent_t *local;
    unsigned epoch;

    if (node == my) {
        /* What a good day! */
        return ent;
    }

    local = pnoptr_cvt_node(ent, my, node);
    epoch = global_epoch;
    if (epoch > local->epoch + 1) {
        /* At lease one epoch in between. It's invalidated. */
        memcpy(local, ent, sizeof(pnoent_t));
        local->epoch = epoch;
    }

    return local;
}

static pnoent_t *pnode_ent(pnoid_t pno, unsigned i) {
    pnoent_t *ent = (pnoent_t *) pnode_get_blk(pno, i / PNODE_NUM_ENT_BLK + 1) + i % PNODE_NUM_ENT_BLK;
    return ent_fetch(ent, pnode_node(pno));
}

static pnoid_t alloc_pnode(int node) {
    struct data_layer *d_layer = DATA(bonsai);
    union pnoid_u id;

    do {
        id.id = d_layer->free_list;
    } while (!cmpxchg2(&d_layer->free_list, id.id, pnode_meta(id.id)->next));
    id.node = node;

    return id.id;
}

static int ent_compare(const void *p, const void *q) {
    const pnoent_t *a = p, *b = q;
    return pkey_compare(a->k, b->k);
}

static inline void pnode_persist(pnoid_t pnode, int fence) {
    unsigned i;
    void *blk;
    for (i = 0; i < PNODE_NUM_BLK; i++) {
        blk = pnode_get_blk(pnode, i);
        bonsai_flush(blk, PNODE_INTERLEAVING_SIZE, 0);
    }
    if (fence) {
        persistent_barrier();
    }
}

static void gen_fgprt(pnoid_t pnode) {
    mnode_t *mno = pnode_meta(pnode);
    unsigned long validmap = mno->validmap;
    unsigned pos;
    for_each_set_bit(pos, &validmap, PNODE_FANOUT) {
        mno->fgprt[pos] = pkey_get_signature(pnode_ent(pnode, pos)->k);
    }
}

static void pnode_init_from_arr(pnoid_t pnode, const pnoent_t *ents, unsigned n) {
    unsigned i;
    pnode_meta(pnode)->validmap = (1ul << n) - 1;
    for (i = 0; i < n; i++) {
        *pnode_ent(pnode, i) = ents[i];
    }
    gen_fgprt(pnode);
}

static void arr_init_from_pnode(pnoent_t *ents, pnoid_t pnode, unsigned *n) {
    unsigned long validmap = pnode_meta(pnode)->validmap;
    unsigned pos, cnt = 0;

    /* Collect all the entries. */
    for_each_set_bit(pos, &validmap, PNODE_FANOUT) {
        ents[cnt++] = *pnode_ent(pnode, pos);
    }

    /* Sort them. */
    qsort(ents, cnt, sizeof(*ents), ent_compare);

    *n = cnt;
}

static void pnode_copy(pnoid_t dst, pnoid_t src) {
    void *dstb, *srcb;
    unsigned i;
    for (i = 0; i < PNODE_NUM_BLK; i++) {
        dstb = pnode_get_blk(dst, i);
        srcb = pnode_get_blk(src, i);
        memcpy(dstb, srcb, PNODE_INTERLEAVING_SIZE);
    }
}

/*
 * SR operation
 *
 * Split the @pnode to @pnode and @sibling. The >=cut part will be moved to @sibling.
 * They will be both recolored against @lc and @rc. (@pnode -> @lc, @sibling -> @rc)
 *
 * If @sibling == NULL, that means recolor only. @pnode will be recolored to @lc.
 */
void pnode_split_and_recolor(pnoid_t *pnode, pnoid_t *sibling, pkey_t *cut, int lc, int rc) {
    pnoid_t original = *pnode, l, r = PNOID_NULL;
    struct data_layer *layer = DATA(bonsai);
    pnoent_t ents[PNODE_FANOUT], *ent;
    mnode_t *mno, *lmno, *rmno;
    unsigned pos, cnt = 0;
    int cmp;

    /* Recolor only. */
    if (!sibling) {
        l = alloc_pnode(lc);
        pnode_copy(l, original);
        goto done;
    }

    arr_init_from_pnode(ents, original, &cnt);

    /* Find split position. */
    for (pos = 0; pos < cnt; pos++) {
        ent = &ents[pos];
        if ((cmp = pkey_compare(ent->k, *cut)) >= 0) {
            break;
        }
    }
    assert(!cmp && pos > 0);

    /* Create pnodes and initialize them. */
    l = alloc_pnode(lc);
    r = alloc_pnode(rc);

    mno = pnode_meta(original);
    lmno = pnode_meta(l);
    rmno = pnode_meta(r);

    pnode_init_from_arr(l, ents, pos);
    pnode_init_from_arr(r, ents + pos, cnt - pos);

    lmno->lfence = mno->lfence;
    rmno->lfence = *cut;

    lmno->next = r;
    rmno->next = mno->next;

    /* Persist and save the right node. */
    pnode_persist(r, 0);
    *sibling = r;

done:
    /* Persist and save the left node. */
    pnode_persist(l, 0);
    *pnode = l;

    persistent_barrier();

    /* Link @l to the pnode list. */
    if (likely(lmno->prev)) {
        pnode_meta(lmno->prev)->next = l;
    } else {
        layer->sentinel = l;
    }
}

static unsigned pnode_find_(pnoent_t *ent, pnoid_t pnode, pkey_t key, uint8_t *fgprt, __le64 validmap) {
    __m128i x = _mm_set1_epi8((char) pkey_get_signature(key));
    __m128i y = _mm_load_si128((const __m128i *) fgprt);
    unsigned long cmp = _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) & validmap;
    unsigned pos;
    for_each_set_bit(pos, &cmp, PNODE_FANOUT) {
        *ent = *pnode_ent(pnode, pos);
        if (likely(!pkey_compare(key, ent->k))) {
            return pos;
        }
    }
    return NOT_FOUND;
}

static inline unsigned pnode_find(pnoid_t pnode, pkey_t key) {
    mnode_t *mno = pnode_meta(pnode);
    pnoent_t ent;
    return pnode_find_(&ent, pnode, key, mno->fgprt, mno->validmap);
}

static void flush_ents(pnoid_t pnode, unsigned long changemap) {
    unsigned pos;
    changemap = (changemap | (changemap >> 1)) & 0x5555555555555555;
    for_each_set_bit(pos, &changemap, PNODE_FANOUT) {
        bonsai_flush(pnode_ent(pnode, pos), sizeof(pnoent_t), 0);
    }
}

static size_t pnode_run_nosmo(pnoid_t pnode, pbatch_op_t *ops) {
    mnode_t *mno = pnode_meta(pnode);
    unsigned long validmap = mno->validmap, changemap = 0;
    size_t insert_cnt = 0;
    pbatch_op_t *op;
    unsigned pos;

    /*
     * Scan @ops, and perform all the update and remove operations,
     * as they do not incur SMO.
     */
    for (op = ops; op->type; op++) {
        assert(!op->done);

        pos = pnode_find(pnode, op->key);

        /* Remove */
        if (op->type == PBO_REMOVE) {
            if (pos != NOT_FOUND) {
                __clear_bit(pos, &validmap);
            }

            op->done = 1;
            continue;
        }

        /* Update */
        if (op->type == PBO_INSERT && pos != NOT_FOUND) {
            pnode_ent(pnode, pos)->v = op->val;
            __set_bit(pos, &changemap);

            op->done = 1;
            continue;
        }

        /* Insert new */
        assert(op->type == PBO_INSERT);
        insert_cnt++;
    }

    mno->validmap = validmap;

    /*
     * Persist those operations. Note that if crash, these updates
     * can be safely redone by oplog.
     */
    flush_ents(pnode, changemap);
    bonsai_flush(&mno->validmap, sizeof(__le64), 1);
}

static void pnode_inplace_insert(pnoid_t pnode, pbatch_op_t *ops) {
    mnode_t *mno = pnode_meta(pnode);
    unsigned long validmap = mno->validmap, changemap = 0;
    pbatch_op_t *op;
    unsigned pos;
    pnoent_t *e;

    for (op = ops; op->type; op++) {
        if (op->done) {
            continue;
        }

        pos = find_first_zero_bit(&validmap, PNODE_FANOUT);
        assert(pos < PNODE_FANOUT);

        e = pnode_ent(pnode, pos);
        e->k = op->key;
        e->v = op->val;

        __set_bit(pos, &changemap);
    }

    flush_ents(pnode, changemap);
    persistent_barrier();

    validmap |= changemap;
    mno->validmap = validmap;
    bonsai_flush(&mno->validmap, sizeof(__le64), 1);
}

static void pnode_partial_rebuild(pnoid_t pnode, pbatch_op_t *ops, size_t tot) {
    struct data_layer *d_layer = DATA(bonsai);

    pnoid_t head, tail, next, prev = PNOID_NULL;
    mnode_t *head_mno, *tail_mno, *mno;
    pnoent_t merged[PNODE_FANOUT], *m;
    pnoent_t ents[PNODE_FANOUT], *ent;
    int node = pnode_node(pnode);
    unsigned cnt, mcnt, i;
    pbatch_op_t *op;

    arr_init_from_pnode(ents, pnode, &cnt);

    for (; tot; tot -= mcnt) {
        mcnt = tot >= PNODE_FANOUT ? PNODE_FANOUT / 2 : tot;
        m = merged;
        for (i = 0; i < mcnt; i++) {
            if (!op->type || (ent < ents + cnt && pkey_compare(ent->k, op->key) < 0)) {
                *m++ = *ent++;
            } else {
                *m++ = (pnoent_t) { op->key, op->val, 0 };
                while ((++op)->done);
            }
        }

        tail = alloc_pnode(node);
        tail_mno = pnode_meta(tail);
        pnode_init_from_arr(tail, m, mcnt);
        if (unlikely(prev == PNOID_NULL)) {
            head = tail;
            tail_mno->lfence = pnode_meta(pnode)->lfence;
        } else {
            mno = pnode_meta(prev);
            tail_mno->lfence = mno->rfence = m[0].k;
            mno->next = tail;
            tail_mno->prev = prev;
            pnode_persist(prev, 0);
        }

        prev = tail;
    }

    tail_mno->rfence = pnode_meta(pnode)->rfence;
    pnode_persist(tail, 0);

    persistent_barrier();

    head_mno = pnode_meta(head);
    mno = pnode_meta(pnode);

    /*
     * Now we've done partial rebuild, we need to link it to the
     * global list. Note that we only need to guarantee the dur-
     * ability of @next. @prev can be set during recovery stage.
     */
    /* TODO: Fine-grained locking */
    spin_lock(&d_layer->plist_lock);

    head_mno->prev = prev = mno->prev;
    tail_mno->next = next = mno->next;
    bonsai_flush(&tail_mno->next, sizeof(pnoid_t), 1);

    if (unlikely(prev == PNOID_NULL)) {
        d_layer->sentinel = head;
    } else {
        mno = pnode_meta(prev);
        mno->next = head;
        /* The durability point. */
        bonsai_flush(&mno->next, sizeof(pnoid_t), 1);
    }

    if (likely(next != PNOID_NULL)) {
        mno = pnode_meta(next);
        mno->prev = tail;
    }

    spin_unlock(&d_layer->plist_lock);
}

void pnode_run_batch(pnoid_t pnode, pbatch_op_t *ops) {
    unsigned long validmap;
    size_t tot;

    tot = pnode_run_nosmo(pnode, ops);

    /* Precalculate total number of entries of the @pnode. */
    validmap = pnode_meta(pnode)->validmap;
    tot += bitmap_weight(&validmap, PNODE_FANOUT);

    /* Inplace insert or partial rebuild? */
    if (tot <= PNODE_FANOUT) {
        pnode_inplace_insert(pnode, ops);
    } else {
        pnode_partial_rebuild(pnode, ops, tot);
    }
}

int pnode_lookup(pnoid_t pnode, pkey_t key, pval_t *val) {
    mnode_t *mno = pnode_meta(pnode);
    uint8_t fgprt[PNODE_FANOUT];
    unsigned long validmap;
    pnoent_t ent;
    int ret = 0;

    validmap = mno->validmap;
    memcpy(fgprt, mno->fgprt, PNODE_FANOUT);

    if (unlikely(pnode_find_(&ent, pnode, key, fgprt, validmap) == NOT_FOUND)) {
        ret = -ENOENT;
    } else {
        *val = ent.v;
    }

    return ret;
}

int is_in_pnode(pnoid_t pnode, pkey_t key) {
    mnode_t *mno = pnode_meta(pnode);
    return pkey_compare(key, mno->lfence) >= 0 && pkey_compare(key, mno->rfence) < 0;
}
