/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 		   Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
#include "cpu.h"

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

#define INODE_FANOUT    16

#define NOT_FOUND       (-1u)
#define NULL_OFF        (-1u)

/* Used for debugging. */
// #define INODE_LFENCE

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

#ifdef INODE_LFENCE
    pkey_t   lfence;
    /* Padding the inode to 4 cachelines. */
    char     padding1[2 * CACHELINE_SIZE - sizeof(pkey_t)];
#endif
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
    struct smo_log log = {0};
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

		/* FIXME: init log.v */
		log.v = NULL;
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

static inline inode_t *inode_seek(pkey_t key, int w, int may_lookup_pnode, pkey_t *ilfence) {
    struct index_layer *i_layer = INDEX(bonsai);
    inode_t *inode;
    pnoid_t pnode;
    void *pptr;

    pptr = i_layer->lookup(i_layer->index_struct, pkey_to_str(key).key, KEY_LEN, ilfence ? ilfence->key : NULL);
    unpack_pptr(&inode, &pnode, pptr);

    if (ilfence) {
        *ilfence = str_to_pkey(*ilfence);
    }

    if (w) {
        inode_prefetch(cache_prefetchw_high, inode);
    } else {
        inode_prefetch(cache_prefetchr_high, inode);
    }

    if (may_lookup_pnode) {
        pnode_prefetch_meta(pnode);
    }

    return inode;
}

static inline void inode_lock(inode_t *inode) {
    spin_lock(&inode->lock);
}

static inline void inode_unlock(inode_t *inode) {
    spin_unlock(&inode->lock);
}

static int inode_crab_and_lock(inode_t **inode, pkey_t key, pkey_t *ilfence) {
    inode_t *target;
    inode_lock(*inode);
    if (unlikely((*inode)->deleted)) {
        /* It's deleted. */
        inode_unlock(*inode);
        return -EAGAIN;
    }
    while (pkey_compare(key, (*inode)->fence) >= 0) {
        if (ilfence) {
            *ilfence = (*inode)->fence;
        }
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

static inline void inode_split_unlock_correct(inode_t **inode, pkey_t key) {
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
#ifdef INODE_LFENCE
    inode->lfence = MIN_KEY;
#endif

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

struct log_info {
    pkey_t key;
    unsigned pos;
};

static int log_info_compare(const void *p, const void *q) {
    const struct log_info *x = p, *y = q;
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
 * @inode's original responsible key range [l, fence) will be split to [l, cut) and [cut, fence).
 * Both @inode and its successor N will be locked.
 */
static void inode_split(inode_t *inode, pkey_t *cut) {
    struct index_layer *i_layer = INDEX(bonsai);

    unsigned long validmap = inode->validmap, lmask = 0;
    struct log_info logs[INODE_FANOUT], *log;
    unsigned pos, cnt = 0;
    uint16_t lvmp, rvmp;
    pkey_t fence;
    inode_t *n;
    void *pptr;

    /* Collect all the logs. */
    for_each_set_bit(pos, &validmap, INODE_FANOUT) {
        log = &logs[cnt++];
        log->key = log_get_key(inode->logs[pos], inode->pno);
        log->pos = pos;
    }

    /* Sort all logs. */
    qsort(logs, cnt, sizeof(struct log_info), log_info_compare);

    /* Get new validmaps: @lvmp and @rvmp. */
    if (cut) {
        for (pos = 0; pos < cnt; pos++) {
            log = &logs[pos];
            if (pkey_compare(log->key, *cut) >= 0) {
                break;
            }
            __set_bit(log->pos, &lmask);
        }
        fence = *cut;
    } else {
        /* Use median. */
        for (pos = 0; pos < cnt / 2; pos++) {
            __set_bit(logs[pos].pos, &lmask);
        }
        fence = logs[pos].key;
    }
    lvmp = lmask;
    rvmp = inode->validmap & ~lmask;

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

#ifdef INODE_LFENCE
    n->lfence = fence;
#endif

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
    inode = inode_seek(key, 1, 0, NULL);

    ret = inode_crab_and_lock(&inode, key, NULL);
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
            inode_split(inode, NULL);
            inode_split_unlock_correct(&inode, key);

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

static int ent_cmp(const void *a, const void *b) {
    const pentry_t *e1 = a, *e2 = b;
    return pkey_compare(e1->k, e2->k);
}

int shim_scan(pkey_t start, int range, pval_t *values) {
    pentry_t ents[INODE_FANOUT], pents[PNODE_FANOUT], *ent, *pent = NULL, t;
    int nr_ents, nr_pents = 0, has_ent, has_pent, cmp, nr_value = 0;
    pnoid_t pno, last_pno = PNOID_NULL;
    unsigned long validmap;
    inode_t *inode, *next;
    unsigned int seq;
    unsigned pos;
    pkey_t fence;

    inode = inode_seek(start, 0, 0, NULL);

scan_inode:
    seq = read_seqcount_begin(&inode->seq);

    fence = ACCESS_ONCE(inode->fence);
    next = inode_off2ptr(ACCESS_ONCE(inode->next));

    /* Get a consistent <fence, next> pair. */
    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto scan_inode;
    }

    /* Walk to correct inode. */
    if (unlikely(pkey_compare(start, fence) >= 0)) {
        inode = next;
        goto scan_inode;
    }

    validmap = ACCESS_ONCE(inode->validmap);
    pno = ACCESS_ONCE(inode->pno);

    nr_ents = 0;
    for_each_set_bit(pos, &validmap, INODE_FANOUT) {
        t = oplog_get(inode->logs[pos])->o_kv;
        if (pkey_compare(t.k, start) >= 0) {
            ents[nr_ents++] = t;
        }
    }
    ent = ents;

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto scan_inode;
    }

    qsort(ents, nr_ents, sizeof(*ents), ent_cmp);

    if (pno != last_pno) {
        /* Not have same pno as the last ino, re-retrieve the pnode. */
        pent = pents;
        nr_pents = pnode_snapshot(pno, pent);
        qsort(pent, nr_pents, sizeof(*pent), ent_cmp);
        last_pno = pno;
    }

    /* [start, fence) */
    while (nr_pents > 0 && pkey_compare(pent->k, start) < 0) {
        nr_pents--;
        pent++;
    }

    /* Merge sort @pents and @ents, and call scanner. */
    goto merge_sort;
    do {
        if (has_ent && has_pent) {
            cmp = pkey_compare(ent->k, pent->k);
            if (cmp < 0) {
                goto use_ent;
            } else if (cmp > 0) {
                goto use_pent;
            }
            while (nr_pents > 0 && pkey_compare(pent->k, ent->k) != 0) {
                nr_pents--;
                pent++;
            }
            goto use_ent;
        } else if (has_ent) {
use_ent:
            values[nr_value++] = ent->v;
            ent++;
        } else {
use_pent:
            values[nr_value++] = pent->v;
            nr_pents--;
            pent++;
        }
        if (nr_value >= range) {
            goto out;
        }
merge_sort:
        has_ent = ent < ents + nr_ents;
        has_pent = nr_pents > 0 && pkey_compare(pent->k, fence) < 0;
    } while (has_ent || has_pent);

    if (unlikely(!next)) {
        goto out;
    }

    /* @inode exhausted, go next. */
    inode = next;
    goto scan_inode;

out:
    return 0;
}

static inline int go_down(pnoid_t pnode, pkey_t key, pval_t *val) {
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

    inode = inode_seek(key, 0, 1, NULL);

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
    return inode_seek(key, 0, 0, NULL)->pno;
}

static inline void sync_inode_pno(inode_t *prev, inode_t *inode, pkey_t ilfence, pnoid_t pno) {
    struct index_layer *i_layer = INDEX(bonsai);
    void *pptr;

    /* update pno */
    inode->pno = pno;
    pack_pptr(&pptr, inode, pno);
    i_layer->insert(i_layer->index_struct, pkey_to_str(ilfence).key, KEY_LEN, pptr);

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
    int ret;

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

        ret = i_layer->remove(i_layer->index_struct, pkey_to_str(old_fence).key, KEY_LEN);
        assert(!ret);

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
    if (!(*inode)->deleted) {
        if (likely(*prev)) {
            inode_unlock(*prev);
        }
        *prev = *inode;
    } else {
        assert(*prev && (*prev)->next == (*inode)->next);
        inode_unlock(*inode);
    }
    *inode = next;
}

/* If you can make sure that the whole @inode is under @pno, you can call @sync_inode. */
static void sync_inode(log_state_t *lst, inode_t *prev, inode_t *inode, pkey_t ilfence, pnoid_t pno) {
    /* Sync the pno pointer inside inode, and the index layer. And pfence. */
    sync_inode_pno(prev, inode, ilfence, pno);
    /* Cleanup old log entries inside @inode. */
    sync_inode_logs(lst, prev, inode);
}

int shim_sync(log_state_t *lst, pnoid_t start, pnoid_t end) {
    inode_t *inode, *prev = NULL;
    /* PNOID_NULL here means @start's predecessor. */
    pnoid_t pno = PNOID_NULL;
    pkey_t ilfence, prfence;
    int ret;

    /* Think it as the prfence of @start's predecessor. */
    prfence = pnode_get_lfence(start);

relookup:
    inode = inode_seek(prfence, 1, 0, &ilfence);

    ret = inode_crab_and_lock(&inode, prfence, &ilfence);
    if (unlikely(ret == -EAGAIN)) {
        /* The inode has been deleted. */
        goto relookup;
    }

    while (inode) {
        if (pkey_compare(prfence, inode->fence) < 0) {
            /*
             *    ilfence
             *    v
             *    |**********|**********|**********|**********|
             *    ^
             *    prfence
             *
             *    ilfence
             *    v
             *    |**********|**********|**********|**********|
             *          ^
             *          next prfence
             */
            if (!pkey_compare(ilfence, prfence)) {
                goto next_pno;
            }

            /*
             * Now our prfence lies somewhere inside inode. We try to split
             * the @inode using prfence as the cut.
             *
             *    ilfence
             *    v
             *    |**********|**********|**********|**********|
             *        ^
             *        prfence
             *
             *    ilfence
             *    v
             *    |***|*******|**********|**********|**********|
             *         ^
             *         prfence
             *
             *        ilfence
             *        v
             *    |===|*******|**********|**********|**********|
             *            ^
             *            next prfence
             *
             */
            inode_split(inode, &prfence);

            /*
             * After split, the remaining @inode contains keys that are strictly
             * smaller than prfence. We can sync it.
             */
            if (likely(pno != PNOID_NULL)) {
                sync_inode(lst, prev, inode, ilfence, pno);
            }

            ilfence = inode->fence;
            sync_moveon(&prev, &inode, 0);

next_pno:
            if (pno == end) {
                break;
            }
            pno = pno == PNOID_NULL ? start : pnode_next(pno);
            prfence = pnode_get_rfence(pno);
        } else {
            /*
             * Oh, prfence >= irfence. The whole inode is under @pno.
             *
             *         irfence
             *               v
             *    |**********|**********|**********|**********|
             *                  ^
             *                  prfence
             *
             *                    irfence
             *                          v
             *    |==========|**********|**********|**********|
             *                  ^
             *                  prfence
             */
            sync_inode(lst, prev, inode, ilfence, pno);

            ilfence = inode->fence;
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
