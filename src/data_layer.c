/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <unistd.h>
#include <numa.h>
#include <sys/time.h>

#include "data_layer.h"
#include "log_layer.h"
#include "bonsai.h"
#include "arch.h"
#include "bitmap.h"

#define PNODE_FANOUT            40
#define PNODE_INTERLEAVING_SIZE 256

#define PNODE_NUM_ENT_BLK       (PNODE_FANOUT * sizeof(pentry_t) / PNODE_INTERLEAVING_SIZE)
#define PNODE_NUM_BLK           (PNODE_NUM_ENT_BLK + 1)

#define NOT_FOUND               (-1u)

#define PNODE_NUM               (DATA_REGION_SIZE / NUM_DIMM / PNODE_INTERLEAVING_SIZE)

#define EPOCH_INTERVAL_US       10000

struct oplog;

typedef struct mnode {
    /* cacheline 0 */
    __le64       validmap;
    __le8        fgprt[PNODE_FANOUT];
    char         padding[16];

    /* cacheline 1 */
    pnoid_t      next; /* pnode list next */
	union {
		pnoid_t prev; /* pnode list prev */
		pnoid_t	node; /* free list next */
	} u;
    /* [lfence, rfence) */
    pkey_t       lfence, rfence;
} mnode_t;

typedef struct dnode {
    pentry_t     ents[PNODE_INTERLEAVING_SIZE / sizeof(pentry_t)];
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
        uint32_t blk_nr         : 29;
		uint32_t numa_node      : 3;
    };
    pnoid_t id;
};

int pnode_numa_node(pnoid_t pno) {
    union pnoid_u u = { .id = pno };
    return (int) u.numa_node;
}

static inline unsigned long pnode_off(pnoid_t pno) {
    union pnoid_u u = { .id = pno };
    return u.blk_nr * (unsigned long) PNODE_INTERLEAVING_SIZE;
}

static inline int *pnode_permute(pnoid_t pno) {
    return pnode_dimm_permute[pno % PNODE_NUM_PERMUTE];
}

static inline void *pnode_dimm_addr(pnoid_t pno, int dimm_idx) {
    int node = pnode_numa_node(pno), dimm = node_to_dimm(node, dimm_idx);
    return DATA(bonsai)->region[dimm].d_start + pnode_off(pno);
}

static inline void *pnode_get_blk(pnoid_t pno, unsigned blk) {
    return pnode_dimm_addr(pno, pnode_permute(pno)[blk]);
}

static mnode_t *pnode_meta(pnoid_t pno) {
    return pnode_get_blk(pno, 0);
}

static inline void *pnoptr_cvt_node(void *ptr, int to_node, int from_node, int dimm_idx) {
    struct data_layer *d_layer = DATA(bonsai);
    int src_dimm, dst_dimm;
    src_dimm = node_to_dimm(from_node, dimm_idx);
    dst_dimm = node_to_dimm(to_node, dimm_idx);
    return d_layer->region[dst_dimm].d_start + (ptr - (void *) d_layer->region[src_dimm].d_start);
}

static pentry_t *pnode_ent(pnoid_t pno, unsigned i) {
    struct data_layer *d_layer = DATA(bonsai);

    unsigned blknr = i / PNODE_NUM_ENT_BLK + 1, blkoff = i % PNODE_NUM_ENT_BLK;
    int my = get_numa_node(__this->t_cpu), node = pnode_numa_node(pno);
    int dimm_idx = pnode_permute(pno)[blknr];
    union pnoid_u pnoid = { .id = pno };
    unsigned epoch, *e, old_e;
    pentry_t *local, *ent;

    ent = pnode_dimm_addr(pno, dimm_idx) + blkoff;

    if (node == my) {
        /* What a good day! */
        return ent;
    }

    epoch = d_layer->epoch;

    local = pnoptr_cvt_node(ent, my, node, dimm_idx);
    e = &d_layer->epoch_table[my][pnoid.blk_nr * (unsigned) PNODE_FANOUT + i];
    old_e = *e;

    if (unlikely(epoch > old_e + 1)) {
        /* At lease one epoch in between. It's invalidated. */
        memcpy(local, ent, sizeof(pentry_t));
        barrier();
    }

    if (unlikely(epoch != old_e)) {
        *e = epoch;
    }

    return local;
}

static pnoid_t alloc_pnode(int node) {
    struct data_layer *d_layer = DATA(bonsai);
    union pnoid_u id;

    do {
        id.id = ACCESS_ONCE(d_layer->free_list);
    } while (!cmpxchg2(&d_layer->free_list, id.id, pnode_meta(id.id)->u.node));
    id.numa_node = node;

    return id.id;
}

static void delay_free_pnode(pnoid_t pnode) {
    struct data_layer *d_layer = DATA(bonsai);
    mnode_t *mno = pnode_meta(pnode);
    pnoid_t head;

    do {
        mno->u.node = head = ACCESS_ONCE(d_layer->tofree_head);
    } while (!cmpxchg2(&d_layer->tofree_head, head, pnode));

    if (head == PNOID_NULL) {
        d_layer->tofree_tail = pnode;
    }
}

static int ent_compare(const void *p, const void *q) {
    const pentry_t *a = p, *b = q;
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

static void pnode_init_from_arr(pnoid_t pnode, const pentry_t *ents, unsigned n) {
    unsigned i;
    pnode_meta(pnode)->validmap = (1ul << n) - 1;
    for (i = 0; i < n; i++) {
        *pnode_ent(pnode, i) = ents[i];
    }
    gen_fgprt(pnode);
}

static void arr_init_from_pnode(pentry_t *ents, pnoid_t pnode, unsigned *n) {
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
    pentry_t ents[PNODE_FANOUT], *ent;
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
    if (likely(lmno->u.prev)) {
        pnode_meta(lmno->u.prev)->next = l;
    } else {
        layer->sentinel = l;
    }

    /* Delay free the original pnode. */
    delay_free_pnode(original);
}

static unsigned pnode_find_(pentry_t *ent, pnoid_t pnode, pkey_t key, uint8_t *fgprt, __le64 validmap) {
    char key_fgprt = (char) pkey_get_signature(key);
    __m256i x32 = _mm256_set1_epi8(key_fgprt), y32 = _mm256_load_si256((const __m256i *) fgprt);
    __m64 x8 = _mm_set1_pi8(key_fgprt), y8 = (__m64) *(unsigned long *) (fgprt + 32);
    unsigned long cmp;
    unsigned pos;
    cmp = (unsigned long) _mm256_movemask_epi8(_mm256_cmpeq_epi8(x32, y32));
    cmp |= (unsigned long) _mm_movemask_pi8(_mm_cmpeq_pi8(x8, y8)) << 32;
    cmp &= validmap;
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
    pentry_t ent;
    return pnode_find_(&ent, pnode, key, mno->fgprt, mno->validmap);
}

static void flush_ents(pnoid_t pnode, unsigned long changemap) {
    unsigned pos;
    changemap = (changemap | (changemap >> 1)) & 0x5555555555555555;
    for_each_set_bit(pos, &changemap, PNODE_FANOUT) {
        bonsai_flush(pnode_ent(pnode, pos), sizeof(pentry_t), 0);
    }
}

static size_t pnode_run_nosmo(pnoid_t pnode, struct list_head *pbatch_list) {
    mnode_t *mno = pnode_meta(pnode);
    unsigned long validmap = mno->validmap, changemap = 0;
    pbatch_cursor_t cursor;
    size_t insert_cnt = 0;
    pbatch_op_t *op;
    unsigned pos;

    /*
     * Scan @ops, and perform all the update and remove operations,
     * as they do not incur SMO.
     */
    for (pbatch_cursor_init(&cursor, pbatch_list); !pbatch_cursor_is_end(&cursor); pbatch_cursor_inc(&cursor)) {
        op = pbatch_cursor_get(&cursor);

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

static void pnode_inplace_insert(pnoid_t pnode, struct list_head *pbatch_list) {
    mnode_t *mno = pnode_meta(pnode);
    unsigned long validmap = mno->validmap, changemap = 0;
    pbatch_cursor_t cursor;
    pbatch_op_t *op;
    unsigned pos;
    pentry_t *e;

    for (pbatch_cursor_init(&cursor, pbatch_list); !pbatch_cursor_is_end(&cursor); pbatch_cursor_inc(&cursor)) {
        op = pbatch_cursor_get(&cursor);

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

static shim_sync_pfence_t *pnode_prebuild(pnoid_t pnode, struct list_head *pbatch_list, size_t tot) {
    struct data_layer *d_layer = DATA(bonsai);

    pnoid_t head, tail, next, prev = PNOID_NULL;
    shim_sync_pfence_t *pfences, *pfence;
    mnode_t *head_mno, *tail_mno, *mno;
    pentry_t merged[PNODE_FANOUT], *m;
    pentry_t ents[PNODE_FANOUT], *ent;
    int node = pnode_numa_node(pnode);
    pbatch_cursor_t cursor;
    unsigned cnt, mcnt, i;
    pbatch_op_t *op;

    pfence = pfences = malloc((tot / (PNODE_FANOUT / 2)) * sizeof(*pfence));

    arr_init_from_pnode(ents, pnode, &cnt);

    for (pbatch_cursor_init(&cursor, pbatch_list); tot; tot -= mcnt) {
        mcnt = tot >= PNODE_FANOUT ? PNODE_FANOUT / 2 : tot;
        m = merged;
        for (i = 0; i < mcnt; i++) {
            op = unlikely(pbatch_cursor_is_end(&cursor)) ? NULL : pbatch_cursor_get(&cursor);
            if (!op || (ent < ents + cnt && pkey_compare(ent->k, op->key) < 0)) {
                *m++ = *ent++;
            } else {
                *m++ = (pentry_t) { op->key, op->val, 0 };
                do {
                    pbatch_cursor_inc(&cursor);
                    op = pbatch_cursor_get(&cursor);
                } while (op->done);
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
            tail_mno->u.prev = prev;
            pnode_persist(prev, 0);
        }

        *pfence++ = (shim_sync_pfence_t) { tail_mno->lfence, tail };

        prev = tail;
    }

    tail_mno->rfence = pnode_meta(pnode)->rfence;
    pnode_persist(tail, 0);

    persistent_barrier();

    *pfence++ = (shim_sync_pfence_t) { tail_mno->rfence, PNOID_NULL };

    head_mno = pnode_meta(head);
    mno = pnode_meta(pnode);

    /*
     * Now we've done @pnode prebuild, we need to link it to the
     * global list. Note that we only need to guarantee the dur-
     * ability of @next. @prev can be set during recovery stage.
     */
    /* TODO: Fine-grained locking */
    spin_lock(&d_layer->plist_lock);

    head_mno->u.prev = prev = mno->u.prev;
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
        mno->u.prev = tail;
    }

    spin_unlock(&d_layer->plist_lock);

    /* Delay free the original pnode. */
    delay_free_pnode(pnode);

    return pfences;
}

void pnode_run_batch(log_state_t *lst, pnoid_t pnode, struct list_head *pbatch_list) {
    mnode_t *mno = pnode_meta(pnode);
    shim_sync_pfence_t nosmo_pfences[] = { { mno->lfence, pnode }, { mno->rfence, PNOID_NULL } };
    shim_sync_pfence_t *pfences = nosmo_pfences;
    unsigned long validmap;
    size_t tot;

    tot = pnode_run_nosmo(pnode, pbatch_list);

    /* Precalculate total number of entries of the @pnode. */
    validmap = mno->validmap;
    tot += bitmap_weight(&validmap, PNODE_FANOUT);

    /* Inplace insert or prebuild? */
    if (tot <= PNODE_FANOUT) {
        pnode_inplace_insert(pnode, pbatch_list);
    } else {
        pfences = pnode_prebuild(pnode, pbatch_list, tot);
    }

    /* Sync with shim layer. */
    shim_sync(lst, pfences);

    if (pfences != nosmo_pfences) {
        free(pfences);
    }
}

pkey_t pnode_get_lfence(pnoid_t pnode) {
    return pnode_meta(pnode)->lfence;
}

void pnode_prefetch_meta(pnoid_t pnode) {
    cache_prefetchr_high(pnode_meta(pnode));
}

int pnode_lookup(pnoid_t pnode, pkey_t key, pval_t *val) {
    mnode_t *mno = pnode_meta(pnode);
    uint8_t fgprt[PNODE_FANOUT];
    unsigned long validmap;
    pentry_t ent;
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

void pnode_recycle() {
    struct data_layer *d_layer = DATA(bonsai);

    if (d_layer->tofree_head == PNOID_NULL) {
        return;
    }

    pnode_meta(d_layer->tofree_tail)->u.prev = d_layer->free_list;
    d_layer->free_list = d_layer->tofree_head;
}

static void init_pnode_pool(struct data_layer *layer) {
    pnoid_t cur;
	int node, num_node_per_socket = PNODE_NUM / NUM_SOCKET;

    for (cur = 0; cur < PNODE_NUM; cur++) {
        pnode_meta(cur)->u.node = cur + 1 < PNODE_NUM ? cur + 1 : PNOID_NULL;
    }
    layer->free_list = 0;
    layer->tofree_head = layer->tofree_tail = PNOID_NULL;
}

static void timer_handler(int sig) {
    struct data_layer *d_layer = DATA(bonsai);
    d_layer->epoch++;
}

static int register_epoch_timer() {
	struct itimerval value;
	struct sigaction sa;
	int ret = 0;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = timer_handler;
	if (sigaction(SIGALRM, &sa, NULL) == -1) {
		perror("sigaction\n");
        goto out;
	}

	value.it_interval.tv_sec = 0;
	value.it_interval.tv_usec = EPOCH_INTERVAL_US;
    value.it_value = value.it_interval;

    ret = setitimer(ITIMER_REAL, &value, NULL);
	if (ret) {
		perror("setitimer\n");
        goto out;
	}

out:
	return ret;
}

void begin_invalidate_unref_entries(unsigned *since) {
    struct data_layer *layer = DATA(bonsai);
    *since = layer->epoch;
}

void end_invalidate_unref_entries(const unsigned *since) {
    struct data_layer *layer = DATA(bonsai);
    while (ACCESS_ONCE(layer->epoch) <= *since + 2) {
        usleep(1000);
    }
}

int data_layer_init(struct data_layer *layer) {
    size_t size = (DATA_REGION_SIZE / sizeof(pentry_t)) * sizeof(unsigned);
    int numa_node, ret;
    unsigned *tab;

    ret = data_region_init(layer);
    if (unlikely(ret)) {
        goto out;
    }
	
    init_pnode_pool(layer);
	
    layer->epoch = 2;
    for (numa_node = 0; numa_node < NUM_SOCKET; numa_node++) {
        tab = numa_alloc_onnode(size, numa_node);
        memset(tab, 0, size);
        layer->epoch_table[numa_node] = tab;
    }

    spin_lock_init(&layer->plist_lock);

    layer->sentinel = PNOID_NULL;

    register_epoch_timer();

	bonsai_print("data_layer_init\n");

out:
    return ret;
}

void data_layer_deinit(struct data_layer* layer) {
	data_region_deinit(layer);

	bonsai_print("data_layer_deinit\n");
}

pnoid_t pnode_sentinel_init() {
    pnoid_t pno = alloc_pnode(0);
    mnode_t *mno = pnode_meta(pno);
    mno->validmap = 0;
    mno->u.prev = mno->next = PNOID_NULL;
    mno->lfence = MIN_KEY;
    mno->rfence = MAX_KEY;
    return pno;
}