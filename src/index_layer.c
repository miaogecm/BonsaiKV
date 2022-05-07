/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 		   Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <assert.h>
#include <malloc.h>
#include <stddef.h>
#include <limits.h>

#include "bonsai.h"
#include "atomic.h"
#include "seqlock.h"
#include "data_layer.h"
#include "arch.h"
#include "index_layer.h"
#include "ordo.h"
#include "cpu.h"

typedef uint32_t lp_t;

#define INODE_FANOUT    16

#define LP_PFENCE       (-1u)
#define NOT_FOUND       (-1u)
#define NULL_OFF        (-1u)

/* Index Node: 2 cachelines */
typedef struct inode {
    /* header, 6-8 words */
    uint16_t validmap;
    uint16_t flipmap;
    uint32_t pfence;

    uint32_t next;
    pnoid_t  pno;

    uint8_t  fgprt[INODE_FANOUT];

    pkey_t   fence;
#ifndef STR_KEY
	char 	 padding[16];
#endif

    spinlock_t lock;
    seqcount_t seq;

    /* packed log/pnode addresses, 8 words */
    lp_t lps[INODE_FANOUT];
} inode_t;

struct pptr {
    union {
        struct {
            uint32_t inode;
            pnoid_t  pnode;
        };
        void *pptr;
    };
} __packed;

static void init_inode_pool(struct inode_pool *pool) {
    void *start = memalign(PAGE_SIZE, INODE_POOL_SIZE);
    inode_t *curr;
    for (curr = start; curr != start + INODE_POOL_SIZE; curr++) {
        curr->next = curr + 1 - (inode_t *) start;
    }
	curr = start;
    curr[TOTAL_INODE - 1].next = NULL_OFF;
    pool->start = pool->freelist = start;
}

static inline uint32_t inode_ptr2off(inode_t *inode) {
    if (unlikely(!inode)) {
        return NULL_OFF;
    }
    return inode - (inode_t *) SHIM(bonsai)->pool.start;
}

static inline inode_t *inode_off2ptr(uint32_t off) {
    if (unlikely(off == NULL_OFF)) {
        return NULL;
    }
    return (inode_t *) SHIM(bonsai)->pool.start + off;
}

static inline void pack_pptr(void **pptr, inode_t *inode, pnoid_t pnode) {
    *pptr = ((struct pptr) {{{ inode_ptr2off(inode), pnode }}}).pptr;
}

static inline void unpack_pptr(inode_t **inode, pnoid_t *pnode, void *pptr) {
    struct pptr p;
    p.pptr = pptr;
    *inode = inode_off2ptr(p.inode);
    *pnode = p.pnode;
}

void do_smo() {
    struct index_layer *i_layer = INDEX(bonsai);
    struct shim_layer* s_layer = SHIM(bonsai);
    unsigned long min_ts;
    struct inode *victim;
    struct smo_log log;
    int cpu, min_cpu;

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

        i_layer->insert(i_layer->index_struct, pkey_to_str(log.k).key, KEY_LEN, log.v);
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

#define inode_prefetch_(prefetcher, t, prefetch_ptr) do { \
        void *(prefetch_ptr);    \
        for ((prefetch_ptr) = (void *) (t); \
             (prefetch_ptr) < (void *) (t) + sizeof(inode_t); \
             (prefetch_ptr) += CACHELINE_SIZE) {    \
            prefetcher(prefetch_ptr);  \
        }                                     \
    } while (0)
#define inode_prefetch(prefetcher, t) \
    inode_prefetch_(prefetcher, t, __UNIQUE_ID(prefetch_ptr))

static inline inode_t *inode_seek(pkey_t key, int w) {
    struct index_layer *i_layer = INDEX(bonsai);
    inode_t *inode;
    pnoid_t pnode;
    void *pptr;

    pptr = i_layer->lookup(i_layer->index_struct, pkey_to_str(key).key, KEY_LEN);
    unpack_pptr(&inode, &pnode, pptr);

    if (w) {
        inode_prefetch(cache_prefetchw_high, inode);
    } else {
        inode_prefetch(cache_prefetchr_high, inode);
    }

    pnode_prefetch_meta(pnode);

    return inode;
}

static inline void inode_lock(inode_t *inode) {
    spin_lock(&inode->lock);
}

static inline void inode_unlock(inode_t *inode) {
    spin_unlock(&inode->lock);
}

static inline int inode_crab_and_lock(inode_t **inode, pkey_t key) {
    inode_t *target;
    inode_lock(*inode);
    if (unlikely(!(*inode)->validmap)) {
        /* It's deleted. */
        inode_unlock(*inode);
        return -EAGAIN;
    }
    while (pkey_compare(key, (*inode)->fence) >= 0) {
        target = inode_off2ptr((*inode)->next);
        inode_lock(target);
        inode_unlock(*inode);
        *inode = target;
    }
    return 0;
}

static inline void inode_split_lock(inode_t **inode, pkey_t key) {
    inode_t *rsibling = inode_off2ptr((*inode)->next);
    if (pkey_compare(key, (*inode)->fence) >= 0) {
        inode_unlock(*inode);
        *inode = rsibling;
    } else {
		inode_unlock(rsibling);
	}
}

static inline inode_t *inode_alloc() {
    struct shim_layer *s_layer = SHIM(bonsai);
    struct inode_pool *pool = &s_layer->pool;
    inode_t *get;

    do {
        get = pool->freelist;
    } while (!cmpxchg2(&pool->freelist, get, inode_off2ptr(get->next)));

    return get;
}

static inline void inode_free(inode_t *inode) {
    struct shim_layer *s_layer = SHIM(bonsai);
    struct inode_pool *pool = &s_layer->pool;
    inode_t *head;

    do {
        head = pool->freelist;
        inode->next = inode_ptr2off(head);
    } while (!cmpxchg2(&pool->freelist, head, inode));
}

static int shim_layer_init() {
    struct shim_layer *layer = SHIM(bonsai);
    int cpu;
	
    init_inode_pool(&layer->pool);
    layer->head = NULL;
	
    atomic_set(&layer->exit, 0);

    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        INIT_KFIFO(layer->fifo[cpu]);
    }
	
	bonsai_print("shim_layer_init\n");
	
    return 0;
}

int shim_sentinel_init(pnoid_t sentinel_pnoid) {
    struct index_layer *i_layer = INDEX(bonsai);
    struct shim_layer *s_layer = SHIM(bonsai);
    inode_t *inode;
    void *pptr;

    inode = inode_alloc();

    pack_pptr(&pptr, inode, sentinel_pnoid);
    i_layer->insert(i_layer->index_struct, pkey_to_str(MIN_KEY).key, KEY_LEN, pptr);

    inode->validmap = 1;
    inode->flipmap = 0;
    inode->pfence = 0;

    inode->next = NULL_OFF;
    inode->pno = sentinel_pnoid;

    inode->fgprt[0] = pkey_get_signature(MIN_KEY);

    inode->fence = MAX_KEY;

    spin_lock_init(&inode->lock);
    seqcount_init(&inode->seq);

    inode->lps[0] = LP_PFENCE;

    s_layer->head = inode;

	printf("sentinel inode: %016lx\n", inode);

    return 0;
}

static inline pkey_t lp_get_key(lp_t lp, pnoid_t pno) {
    if (lp == LP_PFENCE) {
        return pnode_get_lfence(pno);
    } else {
        return oplog_get(lp)->o_kv.k;
    }
}

static inline pval_t lp_get_val(lp_t lp) {
    assert(lp != LP_PFENCE);
    return oplog_get(lp)->o_kv.v;
}

static void set_flip(uint16_t *flipmap, log_state_t *lst, unsigned pos) {
    unsigned long tmp = *flipmap;
    if (lst->flip) {
        __set_bit(pos, &tmp);
    } else {
        __clear_bit(pos, &tmp);
    }
    *flipmap = tmp;
}

struct lp_info {
    pkey_t key;
    unsigned pos;
};

static int lp_compare(const void *p, const void *q) {
    const struct lp_info *x = p, *y = q;
    return pkey_compare(x->key, y->key);
}

static void inode_dump(inode_t *inode) {
    unsigned long vmp = inode->validmap;
    unsigned pos;
    /* TODO: Only integer key. */
    printf(" ====================== [inode %lx] fence=%lx\n",
           (unsigned long) inode, *(unsigned long *) inode->fence.key);
    for_each_set_bit(pos, &vmp, INODE_FANOUT) {
        printf("key[%u]=%lx (%u)\n",
               pos, *(unsigned long *) lp_get_key(inode->lps[pos], inode->pno).key, inode->fgprt[pos]);
    }
}

/*
 * Move @inode's keys within range [cut, fence) to another newly created node. If
 * @cut is NULL, the median value will be chosen. @cut should be an existing key
 * in @inode. If @cut is the minimum key in @inode, the split will not happen, and
 * the return value is 0, otherwise 1. Note that @inode will not be unlocked, and
 * the sibling will be locked.
 */
static int inode_split(inode_t *inode, pkey_t *cut) {
    unsigned long validmap = inode->validmap, lmask = 0;
    struct lp_info lps[INODE_FANOUT], *lp;
    unsigned pos, cnt = 0;
    uint16_t lvmp, rvmp;
    inode_t *right;
    pkey_t fence;
    void *pptr;
    int cmp;

    /* Collect all the lps. */
    for_each_set_bit(pos, &validmap, INODE_FANOUT) {
        lp = &lps[cnt++];
        lp->key = lp_get_key(inode->lps[pos], inode->pno);
        lp->pos = pos;
    }

    /* Sort all lps. */
    qsort(lps, cnt, sizeof(struct lp_info), lp_compare);

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
    rvmp = inode->validmap & ~lmask;

    /* @cut == minimum, no need to split. */
    if (unlikely(!lmask)) {
        return 0;
    }

    /* Alloc and init the right node. */
    right = inode_alloc();
    right->validmap = rvmp;
    right->flipmap = inode->flipmap;
    right->next = inode->next;
    right->pno = inode->pno;
    right->fence = inode->fence;
    spin_lock_init(&right->lock);
    seqcount_init(&right->seq);
    memcpy(right->lps, inode->lps, sizeof(inode->lps));
    memcpy(right->fgprt, inode->fgprt, sizeof(inode->fgprt));
	inode_lock(right);

    /* Set pfence. */
    if (inode->pfence == NOT_FOUND || test_bit(inode->pfence, &lmask)) {
        right->pfence = NOT_FOUND;
    } else {
        right->pfence = inode->pfence;
        inode->pfence = NOT_FOUND;
    }

    /* Now update the left node. */
    write_seqcount_begin(&inode->seq);
    inode->validmap = lvmp;
    inode->next = inode_ptr2off(right);
    inode->fence = fence;
    write_seqcount_end(&inode->seq);

    /* Insert into upper index. */
    pack_pptr(&pptr, right, inode->pno);
    smo_append(fence, pptr);

    return 1;
}

static unsigned inode_find_(lp_t *lp, inode_t *inode, pkey_t key, uint8_t *fgprt, uint32_t validmap) {
    __m128i x = _mm_set1_epi8((char) pkey_get_signature(key));
    __m128i y = _mm_load_si128((const __m128i *) fgprt);
    unsigned long cmp = _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) & validmap;
    unsigned pos;
    for_each_set_bit(pos, &cmp, INODE_FANOUT) {
        *lp = ACCESS_ONCE(inode->lps[pos]);
        if (likely(!pkey_compare(key, lp_get_key(*lp, inode->pno)))) {
            return pos;
        }
    }
    return NOT_FOUND;
}

static inline unsigned inode_find(inode_t *inode, pkey_t key) {
    lp_t lp;
    return inode_find_(&lp, inode, key, inode->fgprt, inode->validmap);
}

/* Insert/update a log key. */
int shim_upsert(log_state_t *lst, pkey_t key, logid_t log) {
    unsigned long validmap;
    inode_t *inode;
    unsigned pos;
    int ret;

relookup:
    inode = inode_seek(key, 1);

    ret = inode_crab_and_lock(&inode, key);
    if (unlikely(ret == -EAGAIN)) {
        /* The inode has been deleted. */
        goto relookup;
    }

    validmap = inode->validmap;

    pos = inode_find(inode, key);
    if (unlikely(pos != NOT_FOUND)) {
        /* Key exists, update. */
        ret = -EEXIST;
    } else {
        pos = find_first_zero_bit(&validmap, INODE_FANOUT);

        if (unlikely(pos == INODE_FANOUT)) {
            /* Inode full, need to split. */
            ret = inode_split(inode, NULL);
            assert(ret);
            inode_split_lock(&inode, key);

            validmap = inode->validmap;
            pos = find_first_zero_bit(&validmap, INODE_FANOUT);
            assert(pos < INODE_FANOUT);
        }

        __set_bit(pos, &validmap);

        ret = 0;
    }

    set_flip(&inode->flipmap, lst, pos);
    inode->lps[pos] = log;
	inode->fgprt[pos] = pkey_get_signature(key);
    barrier();

    inode->validmap = validmap;

    inode_unlock(inode);

    return ret;
}

static int go_down(pnoid_t pnode, pkey_t key, pval_t *val) {
    return pnode_lookup(pnode, key, val);
}

static inline void fgprt_copy(uint8_t *dst, const uint8_t *src) {
    uint64_t *dst_ = (uint64_t *) dst, *src_ = (uint64_t *) src;
    dst_[0] = ACCESS_ONCE(src_[0]);
    dst_[1] = ACCESS_ONCE(src_[1]);
}

int shim_lookup(pkey_t key, pval_t *val) {
    uint8_t fgprt[INODE_FANOUT];
    inode_t *inode, *next;
    uint32_t validmap;
    unsigned int seq;
    pnoid_t pnode;
    unsigned pos;
    pkey_t max;
    lp_t lp;
    int ret;

    inode = inode_seek(key, 0);

retry:
    seq = read_seqcount_begin(&inode->seq);

    max = ACCESS_ONCE(inode->fence);
    next = inode_off2ptr(ACCESS_ONCE(inode->next));

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    if (unlikely(pkey_compare(key, max) >= 0)) {
        inode = next;
        goto retry;
    }

    validmap = ACCESS_ONCE(inode->validmap);
    fgprt_copy(fgprt, inode->fgprt);

    pos = inode_find_(&lp, inode, key, fgprt, validmap);

    if (pos != NOT_FOUND && lp != LP_PFENCE) {
        /* Value in log. */
        *val = lp_get_val(lp);

        ret = 0;
        goto done;
    }

    /* Value in pnode. */
    pnode = ACCESS_ONCE(inode->pno);

    ret = -ENOENT;

done:
    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    if (ret == -ENOENT) {
        ret = go_down(pnode, key, val);
    }

    return ret;
}

pnoid_t shim_pnode_of(pkey_t key) {
    return inode_seek(key, 0)->pno;
}

static inline int inode_remove_old(log_state_t *lst, inode_t *prev, inode_t *inode, pkey_t fence) {
    struct index_layer *i_layer = INDEX(bonsai);
    unsigned long validmap;

    /* Remove unused. */
    validmap = inode->validmap & (inode->flipmap ^ (lst->flip ? 0 : -1u));
    /* Do not delete pfence! */
    if (inode->pfence != NOT_FOUND) {
        /* The log item should be removed, but not the valid bit. */
        inode->lps[inode->pfence] = LP_PFENCE;
        __set_bit(inode->pfence, &validmap);
    }

    barrier();
    inode->validmap = validmap;

    if (!validmap) {
        /* If @inode is empty, ready to free it. */
        write_seqcount_begin(&prev->seq);
        prev->next = inode->next;
        prev->fence = inode->fence;
        write_seqcount_end(&prev->seq);

        i_layer->remove(i_layer->index_struct, pkey_to_str(fence).key, KEY_LEN);

        call_rcu(RCU(bonsai), free, inode);

        return -ENOENT;
    }

    return 0;
}

static inline void inode_update_pno(inode_t *inode, pkey_t fence, pnoid_t pno) {
    void *pptr;
    inode->pno = pno;
    pack_pptr(&pptr, inode, pno);
    smo_append(fence, pptr);
}

static inline void inode_forward(inode_t **prev, inode_t **inode, int lock_next) {
    inode_t *next = inode_off2ptr((*inode)->next);
    if (lock_next && next) {
        inode_lock(next);
    }
    if (likely(*prev)) {
        inode_unlock(*prev);
    }
    *prev = *inode;
    *inode = next;
}

int shim_sync(log_state_t *lst, shim_sync_pfence_t *pfences) {
    shim_sync_pfence_t *p = pfences - 1;
    inode_t *inode, *prev = NULL;
    pkey_t fence;
    unsigned pos;
    int ret;

relookup:
    inode = inode_seek(pfences->pfence, 1);

    ret = inode_crab_and_lock(&inode, pfences->pfence);
    if (unlikely(ret == -EAGAIN)) {
        /* The inode has been deleted. */
        goto relookup;
    }

    while (inode) {
        if (pkey_compare(p[1].pfence, inode->fence) < 0) {
            pos = inode_find(inode, p[1].pfence);
			if (pos == NOT_FOUND) {
				print_key(p[1].pfence);
				inode_dump(inode);
				assert(0);
			}

            if (pos == inode->pfence) {
                /* Obviously the minimum key, no need to split. */
                goto no_split;
            }

            if (unlikely(!inode_split(inode, &p[1].pfence))) {
                /* Still the minimum key... */
                goto no_split;
            }

            if (likely(p >= pfences)) {
                /* Not the initial split. */
                if (!inode_remove_old(lst, prev, inode, fence)) {
                    inode_update_pno(inode, fence, p->pno);
                }
            }

            inode_forward(&prev, &inode, 0);

no_split:
            fence = p[1].pfence;

            inode->pfence = pos;

            if ((++p)->pno == PNOID_NULL) {
                break;
            }
        } else {
            if (!inode_remove_old(lst, prev, inode, fence)) {
                inode_update_pno(inode, fence, p->pno);
            }

            fence = inode->fence;

            inode_forward(&prev, &inode, 1);
        }
    }

    if (prev) {
        inode_unlock(prev);
    }
    if (inode) {
        inode_unlock(inode);
    }

    return 0;
}

void index_layer_init(char* index_name, struct index_layer* layer, init_func_t init,
                      insert_func_t insert, update_func_t update, remove_func_t remove,
				      lookup_func_t lookup, scan_func_t scan, destory_func_t destroy) {
    int node;

    layer->index_struct = init();

	layer->insert       = insert;
    layer->update       = update;
	layer->remove       = remove;
	layer->lookup       = lookup;
	layer->scan         = scan;
	layer->destory      = destroy;

    shim_layer_init();

	bonsai_print("index_layer_init: %s\n", index_name);
}

void index_layer_deinit(struct index_layer* layer) {
	layer->destory(layer->index_struct);

	bonsai_print("index_layer_deinit\n");
}
