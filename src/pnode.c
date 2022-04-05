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
#include <limits.h>
#include <libpmemobj.h>

#include "pnode.h"
#include "oplog.h"
#include "shim.h"
#include "bonsai.h"
#include "cpu.h"
#include "arch.h"
#include "bitmap.h"
#include "ordo.h"

#define NOT_FOUND               (-1u)

#define PNODE_FANOUT            56

#define PNODE_FULL_VALIDMAP     ((1ul << PNODE_FANOUT) - 1)

struct oplog;

static pnode_t *alloc_pnode(int c) {

}

static inline pnoid_t pnode_id(pnode_t *pnode) {

}

static int ent_compare(const void *p, const void *q) {
    const pentry_t *a = p, *b = q;
    return pkey_compare(a->k, b->k);
}

static inline __le64 validmap_lastn(unsigned n) {
    return ((1ul << (PNODE_FANOUT - n)) - 1) ^ PNODE_FULL_VALIDMAP;
}

static inline void pnode_persist(pnode_t *pnode) {
    bonsai_flush(pnode, sizeof(pnode_t), 1);
}

static void gen_fgprt(pnode_t *pnode) {
    unsigned long validmap = pnode->validmap;
    unsigned pos;
    for_each_set_bit(pos, &validmap, PNODE_FANOUT) {
        pnode->fgprt[pos] = pkey_get_signature(pnode->ents[pos].k);
    }
}

/*
 * SR operation
 *
 * Split the @pnode to @pnode and @sibling. The >=cut part will be moved to @sibling.
 * They will be both recolored against @lc and @rc. (@pnode -> @lc, @sibling -> @rc)
 *
 * If @sibling == NULL, that means recolor only. @pnode will be recolored to @lc.
 *
 * Note that the split_and_recolor
 */
void pnode_split_and_recolor(pnode_t **pnode, pnode_t **sibling, pkey_t *cut, int lc, int rc) {
    pnode_t *original = *pnode, *l, *r = NULL, *prev;
    unsigned long validmap = original->validmap;
    struct data_layer *layer = DATA(bonsai);
    pentry_t ents[PNODE_FANOUT], *ent;
    unsigned pos, cnt = 0, ln, rn;
    int cmp;

    /* Recolor only. */
    if (!sibling) {
        l = alloc_pnode(lc);
        memcpy(l, original, sizeof(struct pnode));
        goto done;
    }

    /* Collect all the entries. */
    for_each_set_bit(pos, &validmap, PNODE_FANOUT) {
        ents[cnt++] = original->ents[pos];
    }

    /* Sort them. */
    qsort(ents, cnt, sizeof(*ents), ent_compare);

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

    ln = pos;
    rn = cnt - pos;

    l->validmap = validmap_lastn(ln);
    r->validmap = validmap_lastn(rn);

    memcpy(l->ents + PNODE_FANOUT - ln, ents, sizeof(*ents) * ln);
    memcpy(r->ents + PNODE_FANOUT - rn, ents + pos, sizeof(*ents) * rn);

    gen_fgprt(l);
    gen_fgprt(r);

    l->anchor = original->anchor;
    r->anchor = *cut;

    l->next = pnode_id(r);
    r->next = original->next;

    /* Persist and save the right node. */
    pnode_persist(r);
    *sibling = r;

done:
    /* Persist and save the left node. */
    pnode_persist(l);
    *pnode = l;

    /* Link @l to the pnode list. */
    prev = pnode_get(l->prev);
    if (likely(prev)) {
        prev->next = pnode_id(l);
    } else {
        layer->sentinel = l;
    }
}

static unsigned pnode_find(pnode_t *pnode, pkey_t key) {
    __m128i x = _mm_set1_epi8((char) pkey_get_signature(key));
    __m128i y = _mm_load_si128((const __m128i *) pnode->fgprt);
    unsigned long cmp = _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) & pnode->validmap;
    unsigned pos;
    for_each_set_bit(pos, &cmp, PNODE_FANOUT) {
        if (likely(!pkey_compare(key, pnode->ents[pos].k))) {
            return pos;
        }
    }
    return NOT_FOUND;
}

void pnode_run_batch(pnode_t *pnode, pbatch_op_t *ops) {
    unsigned long validmap = pnode->validmap;
    pbatch_op_t *op;
    unsigned pos;

    /*
     * Scan @ops, and perform all the update and remove operations,
     * as they do not incur SMO.
     */
    for (op = ops; op->type; op++) {
        pos = pnode_find(pnode, op->key);

        if (op->type == PBO_REMOVE) {
            if (pos != NOT_FOUND) {
                __clear_bit(pos, &validmap);
            }

            op->done = 1;
        } else if (op->type == PBO_INSERT && pos != NOT_FOUND) {
            pnode->ents[pos].v = op->val;

            op->done = 1;
        }
    }
}
