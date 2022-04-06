/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Shim Layer: Unified Indexing
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <assert.h>
#include <malloc.h>
#include <stdint.h>
#include <stddef.h>
#include <limits.h>

#include "bonsai.h"
#include "atomic.h"
#include "seqlock.h"
#include "pnode.h"
#include "arch.h"
#include "shim.h"
#include "ordo.h"
#include "cpu.h"

typedef uint32_t lp_t;

#define SNODE_FANOUT    16

#define LP_PFENCE       (-1u)
#define NOT_FOUND       (-1u)
#define NULL_OFF        (-1u)

/* Shim Node: 2 cachelines */
typedef struct snode {
    /* header, 6-8 words */
    uint16_t validmap;
    uint16_t turnmap;
    uint32_t pfence;

    uint32_t next;
    pnoid_t  pno;

    uint8_t  fgprt[SNODE_FANOUT];

    pkey_t   fence;

    spinlock_t lock;
    seqcount_t seq;

    /* packed log/pnode addresses, 8 words */
    lp_t lps[SNODE_FANOUT];
} snode_t;

struct pptr {
    union {
        struct {
            uint32_t snode;
            pnoid_t  pnode;
        };
        void *pptr;
    };
} __packed;

static void init_shim_pool(struct snode_pool *pool) {
    void *start = memalign(PAGE_SIZE, SNODE_POOL_SIZE);
    snode_t *cur;
    for (cur = start; cur != start + SNODE_POOL_SIZE; cur++) {
        cur->next = cur + 1 - (snode_t *) start;
    }
    cur[-1].next = NULL_OFF;
    pool->start = pool->freelist = start;
}

static inline uint32_t snode_ptr2off(snode_t *snode) {
    if (unlikely(!snode)) {
        return NULL_OFF;
    }
    return snode - (snode_t *) SHIM(bonsai)->pool->start;
}

static inline snode_t *snode_off2ptr(uint32_t off) {
    if (unlikely(off == NULL_OFF)) {
        return NULL;
    }
    return (snode_t *) SHIM(bonsai)->pool->start + off;
}

static inline void pack_pptr(void **pptr, snode_t *snode, pnoid_t pnode) {
    *pptr = ((struct pptr) {{{ snode_ptr2off(snode), pnode }}}).pptr;
}

static inline void unpack_pptr(snode_t **snode, struct pnode **pnode, void *pptr) {
    struct pptr p;
    p.pptr = pptr;
    *snode = snode_off2ptr(p.snode);
    *pnode = pnode_get(p.pnode);
}

void do_smo() {
    struct index_layer *i_layer = INDEX(bonsai);
    struct shim_layer* s_layer = SHIM(bonsai);
    unsigned long min_ts;
    struct snode *victim;
    struct smo_log log;
    int cpu, min_cpu;
    void *key, *pptr;
    pnode_t *pno;

    while (1) {
        min_ts = ULONG_MAX;
        min_cpu = -1;
        for (cpu = 0; cpu < NUM_CPU; cpu++) {
            if (kfifo_peek(&s_layer->fifo[cpu], &log)) {
                if (log.ts <= min_ts) {
                    min_ts = log.ts;
                    min_cpu = cpu;
                }
            }
        }

        if (unlikely(min_cpu == -1)) {
            break;
        }

        kfifo_get(&s_layer->fifo[min_cpu], &log);

        key = pkey_to_str(log.k).key;
        if (likely(log.v)) {
            i_layer->insert(i_layer->index_struct, key, KEY_LEN, log.v);
        } else {
            pptr = i_layer->remove(i_layer->index_struct, key, KEY_LEN);
            unpack_pptr(&victim, &pno, pptr);
            /* TODO: Lazy free @victim */
        }
    }
}

static void smo_append(pkey_t k, void *v) {
    struct shim_layer *s_layer = SHIM(bonsai);
    unsigned long ts;
    int ret;

    ts = ordo_new_clock(0);

retry:
    ret = kfifo_put(&s_layer->fifo[__this->t_cpu], ((struct smo_log) { ts, k, v }));
    if (unlikely(!ret)) {
        wakeup_smo();
        goto retry;
    }

    wakeup_smo();
}

#define snode_prefetch_(prefetcher, t, prefetch_ptr) do { \
        void *(prefetch_ptr);    \
        for ((prefetch_ptr) = (void *) (t); \
             (prefetch_ptr) < (void *) (t) + sizeof(snode_t); \
             (prefetch_ptr) += CACHELINE_SIZE) {    \
            prefetcher(prefetch_ptr);  \
        }                                     \
    } while (0)
#define snode_prefetch(prefetcher, t) \
    snode_prefetch_(prefetcher, t, __UNIQUE_ID(prefetch_ptr))

static inline snode_t *snode_seek(pkey_t key, int w) {
    struct index_layer *i_layer = INDEX(bonsai);
    struct pnode *pnode;
    snode_t *snode;
    void *pptr;

    pptr = i_layer->lookup(i_layer->index_struct, pkey_to_str(key).key, KEY_LEN);
    unpack_pptr(&snode, &pnode, pptr);

    if (w) {
        snode_prefetch(cache_prefetchw_high, snode);
    } else {
        snode_prefetch(cache_prefetchr_high, snode);
    }

    /* TODO: Implement pnode prefetch. */
    (void) pnode;

    return snode;
}

static inline void snode_lock(snode_t *snode) {
    spin_lock(&snode->lock);
}

static inline void snode_unlock(snode_t *snode) {
    spin_unlock(&snode->lock);
}

static inline int snode_crab_and_lock(snode_t **snode, pkey_t key) {
    snode_t *target;
    snode_lock(*snode);
    /* TODO: Handle delected snode. */
    while (pkey_compare(key, (*snode)->fence) >= 0) {
        target = snode_off2ptr((*snode)->next);
        snode_lock(target);
        snode_unlock(*snode);
        *snode = target;
    }
    return 0;
}

static inline void snode_crab(snode_t **snode, pkey_t key) {
    snode_t *target;
    while (pkey_compare(key, (*snode)->fence) >= 0) {
        target = snode_off2ptr((*snode)->next);
        snode_unlock(*snode);
        *snode = target;
    }
}

static inline snode_t *snode_alloc() {
    struct shim_layer *s_layer = SHIM(bonsai);
    struct snode_pool *pool = s_layer->pool;
    snode_t *get;

    do {
        get = pool->freelist;
    } while (!cmpxchg2(&pool->freelist, get, snode_off2ptr(get->next)));

    return get;
}

static inline void snode_free(snode_t *snode) {
    struct shim_layer *s_layer = SHIM(bonsai);
    struct snode_pool *pool = s_layer->pool;
    snode_t *head;

    do {
        head = pool->freelist;
        snode->next = snode_ptr2off(head);
    } while (!cmpxchg2(&pool->freelist, head, snode));
}

int shim_layer_init(struct shim_layer *layer) {
    int cpu;

    init_shim_pool(layer->pool);
    layer->head = NULL;

    atomic_set(&layer->exit, 0);

    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        INIT_KFIFO(layer->fifo[cpu]);
    }

    return 0;
}

int shim_sentinel_init(struct shim_layer *layer, pnoid_t sentinel_pnoid) {
    struct index_layer *i_layer = INDEX(bonsai);
    snode_t *snode;
    void *pptr;

    snode = snode_alloc();

    pack_pptr(&pptr, snode, sentinel_pnoid);
    i_layer->insert(i_layer->index_struct, pkey_to_str(MIN_KEY).key, KEY_LEN, pptr);

    layer->head = snode;

    return 0;
}

static inline pkey_t lp_get_key(lp_t lp) {
    if (lp == LP_PFENCE) {
        /* TODO: LP_PFENCE */
    } else {
        return oplog_get(lp)->o_kv.k;
    }
}

static inline pval_t lp_get_val(lp_t lp) {
    assert(lp != LP_PFENCE);
    return oplog_get(lp)->o_kv.v;
}

static void set_turn(uint16_t *turnmap, log_state_t *lst, unsigned pos) {
    unsigned long tmp = *turnmap;
    if (lst->turn) {
        __set_bit(pos, &tmp);
    } else {
        __clear_bit(pos, &tmp);
    }
    *turnmap = tmp;
}

struct lp_info {
    pkey_t key;
    unsigned pos;
};

static int lp_compare(const void *p, const void *q) {
    const struct lp_info *x = p, *y = q;
    return pkey_compare(x->key, y->key);
}

/*
 * Move @snode's keys within range [cut, fence) to another newly created node. If
 * @cut is NULL, the median value will be chosen. @cut should be an existing key
 * in @snode. If @cut is the minimum key in @snode, the split will not happen, and
 * the return value is 0, otherwise 1. Note that @snode will not be unlocked, and
 * the sibling will be locked.
 */
static int snode_split(snode_t *snode, pkey_t *cut) {
    unsigned long validmap = snode->validmap, lmask = 0;
    struct lp_info lps[SNODE_FANOUT], *lp;
    unsigned pos, cnt = 0;
    uint16_t lvmp, rvmp;
    snode_t *right;
    pkey_t fence;
    void *pptr;
    int cmp;

    /* Collect all the lps. */
    for_each_set_bit(pos, &validmap, SNODE_FANOUT) {
        lp = &lps[cnt++];
        lp->key = lp_get_key(snode->lps[pos]);
        lp->pos = pos;
    }

    /* Sort all lps. */
    qsort(lps, cnt, sizeof(lp_t), lp_compare);

    /* Get new validmaps: @lvmp and @rvmp. */
    if (cut) {
        for (pos = 0; pos < cnt; pos++) {
            lp = &lps[pos];
            if ((cmp = pkey_compare(lp->key, *cut)) >= 0) {
                break;
            }
            __set_bit(lp->pos, &lmask);
        }
        /* @cut should be existing key, so the last compare must be equal. */
        assert(!cmp);
        fence = *cut;
    } else {
        /* Use median. */
        for (pos = 0; pos < cnt / 2; pos++) {
            __set_bit(lps[pos].pos, &lmask);
        }
        fence = lps[pos].key;
    }
    lvmp = lmask;
    rvmp = snode->validmap & ~lmask;

    /* @cut == minimum, no need to split. */
    if (unlikely(!lmask)) {
        return 0;
    }

    /* Alloc and init the right node. */
    right = snode_alloc();
    right->validmap = rvmp;
    right->turnmap = snode->turnmap;
    right->next = snode->next;
    right->pno = snode->pno;
    right->fence = snode->fence;
    spin_lock_init(&right->lock);
    seqcount_init(&right->seq);
    memcpy(right->lps, snode->lps, sizeof(snode->lps));
    memcpy(right->fgprt, snode->fgprt, sizeof(snode->fgprt));
    snode_lock(right);

    /* Set pfence. */
    if (snode->pfence == NOT_FOUND || test_bit(snode->pfence, &lmask)) {
        right->pfence = NOT_FOUND;
    } else {
        right->pfence = snode->pfence;
        snode->pfence = NOT_FOUND;
    }

    /* Now update the left node. */
    write_seqcount_begin(&snode->seq);
    snode->validmap = lvmp;
    snode->next = snode_ptr2off(right);
    snode->fence = fence;
    write_seqcount_end(&snode->seq);

    /* Insert into upper index. */
    pack_pptr(&pptr, right, snode->pno);
    smo_append(fence, pptr);

    return 1;
}

static unsigned snode_find_(lp_t *lp, snode_t *snode, pkey_t key, uint8_t *fgprt, uint32_t validmap) {
    __m128i x = _mm_set1_epi8((char) pkey_get_signature(key));
    __m128i y = _mm_load_si128((const __m128i *) fgprt);
    unsigned long cmp = _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) & validmap;
    unsigned pos;
    for_each_set_bit(pos, &cmp, SNODE_FANOUT) {
        *lp = ACCESS_ONCE(snode->lps[pos]);
        if (likely(!pkey_compare(key, lp_get_key(*lp)))) {
            return pos;
        }
    }
    return NOT_FOUND;
}

static inline unsigned snode_find(snode_t *snode, pkey_t key) {
    lp_t lp;
    return snode_find_(&lp, snode, key, snode->fgprt, snode->validmap);
}

/* Insert/update a log key. */
int shim_upsert(log_state_t *lst, pkey_t key, logid_t log) {
    unsigned long validmap;
    snode_t *snode;
    unsigned pos;
    int ret;

relookup:
    snode = snode_seek(key, 1);

    ret = snode_crab_and_lock(&snode, key);
    if (unlikely(ret == -EAGAIN)) {
        /* The snode has been deleted. */
        goto relookup;
    }

    validmap = snode->validmap;

    pos = snode_find(snode, key);
    if (unlikely(pos != NOT_FOUND)) {
        /* Key exists, update. */
        ret = -EEXIST;
    } else {
        pos = find_first_zero_bit(&validmap, SNODE_FANOUT);

        if (unlikely(pos == SNODE_FANOUT)) {
            /* SNode full, need to split. */
            ret = snode_split(snode, NULL);
            assert(ret);
            snode_crab(&snode, key);

            validmap = snode->validmap;
            pos = find_first_zero_bit(&validmap, SNODE_FANOUT);
            assert(pos < SNODE_FANOUT);
        }

        __set_bit(pos, &validmap);

        ret = 0;
    }

    set_turn(&snode->turnmap, lst, pos);
    snode->lps[pos] = log;
    barrier();

    snode->validmap = validmap;

    snode_unlock(snode);

    return ret;
}

static int go_down(struct pnode *pnode, pkey_t key, pval_t *val) {

}

static inline void fgprt_copy(uint8_t *dst, const uint8_t *src) {
    uint64_t *dst_ = (uint64_t *) dst, *src_ = (uint64_t *) src;
    dst_[0] = ACCESS_ONCE(src_[0]);
    dst_[1] = ACCESS_ONCE(src_[1]);
}

int shim_lookup(pkey_t key, pval_t *val) {
    uint8_t fgprt[SNODE_FANOUT];
    snode_t *snode, *next;
    struct pnode *pnode;
    uint32_t validmap;
    unsigned int seq;
    unsigned pos;
    pkey_t max;
    lp_t lp;
    int ret;

    snode = snode_seek(key, 0);

retry:
    seq = read_seqcount_begin(&snode->seq);

    max = ACCESS_ONCE(snode->fence);
    next = snode_off2ptr(ACCESS_ONCE(snode->next));

    if (unlikely(read_seqcount_retry(&snode->seq, seq))) {
        goto retry;
    }

    if (unlikely(pkey_compare(key, max) >= 0)) {
        snode = next;
        goto retry;
    }

    validmap = ACCESS_ONCE(snode->validmap);
    fgprt_copy(fgprt, snode->fgprt);

    pos = snode_find_(&lp, snode, key, fgprt, validmap);

    if (pos != NOT_FOUND && lp != LP_PFENCE) {
        /* Value in log. */
        *val = lp_get_val(lp);

        ret = 0;
        goto done;
    }

    /* Value in pnode. */
    pnode = pnode_get(ACCESS_ONCE(snode->pno));

    ret = -ENOENT;

done:
    if (unlikely(read_seqcount_retry(&snode->seq, seq))) {
        goto retry;
    }

    if (ret == -ENOENT) {
        ret = go_down(pnode, key, val);
    }

    return ret;
}

struct pnode *shim_pnode_of(pkey_t key) {
    return pnode_get(snode_seek(key, 0)->pno);
}

static inline int snode_remove_old(log_state_t *lst, snode_t *prev, snode_t *snode, pkey_t fence) {
    unsigned long validmap;

    /* Remove unused. */
    validmap = snode->validmap & (snode->turnmap ^ (lst->turn ? 0 : -1u));
    /* Do not delete pfence! */
    if (snode->pfence != NOT_FOUND) {
        /* The log item should be removed, but not the valid bit. */
        snode->lps[snode->pfence] = LP_PFENCE;
        __set_bit(snode->pfence, &validmap);
    }

    barrier();
    snode->validmap = validmap;

    if (!validmap) {
        /* If @snode is empty, ready to free it. */
        write_seqcount_begin(&prev->seq);
        prev->next = snode->next;
        prev->fence = snode->fence;
        write_seqcount_end(&prev->seq);

        smo_append(fence, NULL);

        return -ENOENT;
    }

    return 0;
}

static inline void snode_update_pno(snode_t *snode, pkey_t fence, pnoid_t pno) {
    void *pptr;
    snode->pno = pno;
    pack_pptr(&pptr, snode, pno);
    smo_append(fence, pptr);
}

static inline void snode_forward(snode_t **prev, snode_t **snode, int lock_next) {
    snode_t *next;
    next = snode_off2ptr((*snode)->next);
    if (lock_next) {
        snode_lock(next);
    }
    if (likely(*prev)) {
        snode_unlock(*prev);
    }
    *prev = *snode;
    *snode = next;
}

int shim_sync(log_state_t *lst, shim_sync_pfence_t *pfences) {
    shim_sync_pfence_t *p = pfences - 1;
    snode_t *snode, *prev;
    pkey_t fence;
    unsigned pos;
    int ret;

relookup:
    snode = snode_seek(pfences->pfence, 1);

    ret = snode_crab_and_lock(&snode, pfences->pfence);
    if (unlikely(ret == -EAGAIN)) {
        /* The snode has been deleted. */
        goto relookup;
    }

    while (1) {
        if (pkey_compare(p[1].pfence, snode->fence) < 0) {
            pos = snode_find(snode, p[1].pfence);
            assert(pos != NOT_FOUND);

            if (pos == snode->pfence) {
                /* Obviously the minimum key, no need to split. */
                goto no_split;
            }

            if (unlikely(!snode_split(snode, &p[1].pfence))) {
                /* Still the minimum key... */
                goto no_split;
            }

            if (likely(p >= pfences)) {
                /* Not the initial split. */
                if (!snode_remove_old(lst, prev, snode, fence)) {
                    snode_update_pno(snode, fence, p->pno);
                }
            }

            snode_forward(&prev, &snode, 0);

no_split:
            fence = p[1].pfence;

            snode->pfence = pos;

            if (++p->pno == PNOID_NONE) {
                break;
            }
        } else {
            if (!snode_remove_old(lst, prev, snode, fence)) {
                snode_update_pno(snode, fence, p->pno);
            }

            fence = snode->fence;

            snode_forward(&prev, &snode, 1);
        }
    }

    snode_unlock(snode);

    return 0;
}
