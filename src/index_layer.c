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

#define INODE_FANOUT    16

#define NOT_FOUND       (-1u)
#define NULL_OFF        (-1u)

/* Index Node: 2 cachelines */
typedef struct inode {
    /* header, 6-8 words */
    uint16_t validmap;
    uint16_t flipmap;
    uint16_t has_pfence;
    uint16_t deleted;

    uint32_t next;
    pnoid_t  pno;

    uint8_t  fgprt[INODE_FANOUT];

    pkey_t   fence;
#ifndef STR_KEY
	char 	 padding[16];
#endif

    spinlock_t lock;
    seqcount_t seq;

    /* packed log addresses, 8 words */
    logid_t logs[INODE_FANOUT];
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

static inline inode_t *inode_seek(pkey_t key, int w, pkey_t *actual_key) {
    struct index_layer *i_layer = INDEX(bonsai);
    inode_t *inode;
    pnoid_t pnode;
    void *pptr;

    pptr = i_layer->lookup(i_layer->index_struct, pkey_to_str(key).key, KEY_LEN, actual_key ? actual_key->key : NULL);
    unpack_pptr(&inode, &pnode, pptr);

    if (actual_key) {
        *actual_key = str_to_pkey(*actual_key);
    }

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

static int inode_crab_and_lock(inode_t **inode, pkey_t key) {
    inode_t *target;
    inode_lock(*inode);
    if (unlikely((*inode)->deleted)) {
        /* It's deleted. */
        inode_unlock(*inode);
        return -EAGAIN;
    }
    while (pkey_compare(key, (*inode)->fence) >= 0) {
        target = inode_off2ptr((*inode)->next);
        inode_lock(target);
        /*
         * Don't worry. @target must be still alive now, as deletion needs
         * to lock both inode and its predecessor. We're holding the lock
         * of @target's predecessor.
         */
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

    inode->validmap = 0;
    inode->flipmap = 0;
    /* MIN_KEY is the sentinel inode's pfence. */
    inode->has_pfence = 1;
    inode->deleted = 0;

    inode->next = NULL_OFF;
    inode->pno = sentinel_pnoid;

    inode->fence = MAX_KEY;

    spin_lock_init(&inode->lock);
    seqcount_init(&inode->seq);

    s_layer->head = inode;

	printf("sentinel inode: %016lx\n", (unsigned long) inode);

    return 0;
}

static inline pkey_t log_get_key(logid_t log, pnoid_t pno) {
    return oplog_get(log)->o_kv.k;
}

static inline pval_t log_get_val(logid_t log) {
    return oplog_get(log)->o_kv.v;
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
               pos, *(unsigned long *) log_get_key(inode->logs[pos], inode->pno).key, inode->fgprt[pos]);
    }
}

/*
 * Move @inode's keys within range [cut, fence) to another node N.
 *
 * [ Operation ]
 *
 * If @cut is NULL, the median value will be chosen. N will be either a newly created node,
 * which is @inode's new successor, or the original @inode if @cut is lower or equal than all
 * the keys in @inode. The return value could be used to distinguish between these two cases:
 * 1 for the first case, and 0 for the second. Note that in the latter case, @inode_split does
 * nothing.
 *
 * [ Locking ]
 *
 * If split happens (i.e. The return value is 1.), both @inode and @inode's successor will be
 * locked.
 */
static int inode_split(inode_t *inode, pkey_t *cut) {
    struct index_layer *i_layer = INDEX(bonsai);

    unsigned long validmap = inode->validmap, lmask = 0;
    struct lp_info lps[INODE_FANOUT], *lp;
    unsigned pos, cnt = 0;
    uint16_t lvmp, rvmp;
    pkey_t fence;
    inode_t *n;
    void *pptr;

    /* Collect all the logs. */
    for_each_set_bit(pos, &validmap, INODE_FANOUT) {
        lp = &lps[cnt++];
        lp->key = log_get_key(inode->logs[pos], inode->pno);
        lp->pos = pos;
    }

    /* Sort all logs. */
    qsort(lps, cnt, sizeof(struct lp_info), lp_compare);

    /* Get new validmaps: @lvmp and @rvmp. */
    if (cut) {
        for (pos = 0; pos < cnt; pos++) {
            lp = &lps[pos];
            if (pkey_compare(lp->key, *cut) >= 0) {
                break;
            }
            __set_bit(lp->pos, &lmask);
        }
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

    /* Alloc and init the n node. */
    n = inode_alloc();
    n->validmap = rvmp;
    n->flipmap = inode->flipmap;
    n->next = inode->next;
    n->pno = inode->pno;
    n->fence = inode->fence;
    n->deleted = 0;
    spin_lock_init(&n->lock);
    seqcount_init(&n->seq);
    memcpy(n->logs, inode->logs, sizeof(inode->logs));
    memcpy(n->fgprt, inode->fgprt, sizeof(inode->fgprt));
	inode_lock(n);

    /* Note that if an inode contains a pfence key, it should be the minimum key of the inode. */
    n->has_pfence = 0;

    /* Now update @inode. */
    write_seqcount_begin(&inode->seq);
    inode->validmap = lvmp;
    inode->next = inode_ptr2off(n);
    inode->fence = fence;
    write_seqcount_end(&inode->seq);

    /* Insert into upper index. */
    pack_pptr(&pptr, n, inode->pno);
    i_layer->insert(i_layer->index_struct, pkey_to_str(fence).key, KEY_LEN, pptr);

    return 1;
}

static unsigned inode_find_(logid_t *log, inode_t *inode, pkey_t key, uint8_t *fgprt, uint32_t validmap) {
    __m128i x = _mm_set1_epi8((char) pkey_get_signature(key));
    __m128i y = _mm_load_si128((const __m128i *) fgprt);
    unsigned long cmp = _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) & validmap;
    unsigned pos;
    for_each_set_bit(pos, &cmp, INODE_FANOUT) {
        *log = ACCESS_ONCE(inode->logs[pos]);
        if (likely(!pkey_compare(key, log_get_key(*log, inode->pno)))) {
            return pos;
        }
    }
    return NOT_FOUND;
}

static inline unsigned inode_find(inode_t *inode, pkey_t key) {
    logid_t log;
    return inode_find_(&log, inode, key, inode->fgprt, inode->validmap);
}

/* Insert/update a log key. */
int shim_upsert(log_state_t *lst, pkey_t key, logid_t log) {
    unsigned long validmap;
    inode_t *inode;
    unsigned pos;
    int ret;

relookup:
    inode = inode_seek(key, 1, NULL);

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
    inode->logs[pos] = log;
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
    logid_t log;
    pkey_t max;
    int ret;

    inode = inode_seek(key, 0, NULL);

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

    pos = inode_find_(&log, inode, key, fgprt, validmap);

    if (pos != NOT_FOUND) {
        /* Value in log. */
        *val = log_get_val(log);

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
    return inode_seek(key, 0, NULL)->pno;
}

static inline void sync_inode_pno(inode_t *prev, inode_t *inode, pnoid_t pno, pkey_t lbound) {
    struct index_layer *i_layer = INDEX(bonsai);
    void *pptr;

    /* update pno */
    inode->pno = pno;
    pack_pptr(&pptr, inode, pno);
    i_layer->insert(i_layer->index_struct, pkey_to_str(prev ? prev->fence : lbound).key, KEY_LEN, pptr);

    /* update pfence */
    if (prev && prev->pno == pno) {
        /* @inode is not the leader. It shouldn't have pfence. */
        assert(!inode->has_pfence);
        return;
    }
    inode->has_pfence = 1;
}

static int sync_inode_logs(log_state_t *lst, inode_t *prev, inode_t *inode) {
    struct index_layer *i_layer = INDEX(bonsai);
    unsigned long validmap;

    /* Remove entries that belongs to the previous flip. */
    validmap = inode->validmap & (inode->flipmap ^ (lst->flip ? 0 : -1u));

    inode->validmap = validmap;
    barrier();

    /* If no logs and pfence inside @inode, free it. */
    if (unlikely(!validmap && !inode->has_pfence)) {
        /*
         * Note that @prev won't be NULL here. The first @inode to sync
         * should be definitely a leading inode, which has pfence, and
         * won't be deleted.
         */
        pkey_t old_fence = prev->fence;

        inode->deleted = 1;

        write_seqcount_begin(&prev->seq);
        prev->next = inode->next;
        prev->fence = inode->fence;
        write_seqcount_end(&prev->seq);

        i_layer->remove(i_layer->index_struct, pkey_to_str(old_fence).key, KEY_LEN);

        call_rcu(RCU(bonsai), (rcu_cb_t) inode_free, inode);

        return -ENOENT;
    }

    return 0;
}

static inline void sync_moveon(inode_t **prev, inode_t **inode, int lock_next) {
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

/* If you can make sure that the whole @inode is under @pno, you can call @sync_inode. */
static void sync_inode(log_state_t *lst, inode_t *prev, inode_t *inode, pnoid_t pno, pkey_t lbound) {
    /* Sync the pno pointer inside inode, and the index layer. And pfence. */
    sync_inode_pno(prev, inode, pno, lbound);
    /* Cleanup old log entries inside @inode. */
    sync_inode_logs(lst, prev, inode);
}

int shim_sync(log_state_t *lst, pnoid_t start, pnoid_t end) {
    inode_t *inode, *prev = NULL;
    pnoid_t pno = PNOID_NULL;
    pkey_t lbound, rfence;
    int ret;

    /* View it as the rfence of @start's predecessor. */
    rfence = pnode_get_lfence(start);

relookup:
    inode = inode_seek(rfence, 1, &lbound);

    ret = inode_crab_and_lock(&inode, rfence);
    if (unlikely(ret == -EAGAIN)) {
        /* The inode has been deleted. */
        goto relookup;
    }

    while (inode) {
        if (pkey_compare(rfence, inode->fence) < 0) {
            /*
             * Now our rfence lies somewhere inside inode. We try to split
             * the @inode using rfence as the cut.
             */
            if (unlikely(!inode_split(inode, &rfence))) {
                /* Leftmost cut, no split. */
                goto no_split;
            }

            /*
             * After split, the remaining @inode contains keys that are strictly
             * smaller than rfence. We can sync it.
             */
            if (likely(pno != PNOID_NULL)) {
                sync_inode(lst, prev, inode, pno, lbound);
            }

            sync_moveon(&prev, &inode, 0);

no_split:
            if (pno == end) {
                break;
            }
            pno = pno == PNOID_NULL ? start : pnode_next(pno);
            rfence = pnode_get_rfence(pno);
        } else {
            /* Oh, rfence >= ifence. The whole inode is under @pno. */
            sync_inode(lst, prev, inode, pno, lbound);

            sync_moveon(&prev, &inode, 1);
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
