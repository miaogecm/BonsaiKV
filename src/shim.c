/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <numa.h>
#include <assert.h>
#include <limits.h>
#include <malloc.h>

#include "bonsai.h"
#include "arch.h"
#include "shim.h"
#include "rcu.h"
#include "cpu.h"
#include "long_key.h"

extern struct bonsai_info* bonsai;

#define BDTABLE_MAX_KEYN        24
#define MPTABLE_MAX_KEYN        (BDTABLE_MAX_KEYN / 2)
#define SLOTS_SIZE              13

#define MID_POS         (MPTABLE_MAX_KEYN - 1)
/* [from, to) */
#define MT_FROM_POS     0
#define MT_TO_POS       MPTABLE_MAX_KEYN
/* [from, to) */
#define ST_FROM_POS     MT_TO_POS
#define ST_TO_POS       BDTABLE_MAX_KEYN

#define NOT_FOUND           (-1)
#define SIGNATURE_NOENT     0xff

typedef uint32_t mptable_off_t;

typedef struct {
    pkey_t k;
    pval_t *v;
} kvpair_t;

struct mptable {
	/* Header - 6 words */

    /* MT Only: */

    /* @seq protects @slots, @slave, @next, @max, and entries */
    seqcount_t seq;
    /* @slots_seq protects @slots only */
    seqcount_t slots_seq;
    uint8_t slots[SLOTS_SIZE];
    unsigned short generation;
    pkey_t fence, max;
    mptable_off_t slave, next;
    spinlock_t lock;

    /* Both: */

    /* Key signatures - 2 words */
    uint8_t signatures[MPTABLE_MAX_KEYN];

    /* Key-Value pairs - 24 words */
    kvpair_t entries[MPTABLE_MAX_KEYN];
};

struct mptable_desc {
    uint64_t    off;
    uint64_t    prev;
    uint64_t    next;
    char        padding[40];
};

struct mptable_pool_hdr {
    atomic_t used;
    struct mptable_desc* free;
    struct mptable_desc* inuse;
    spinlock_t lock;
};

struct mptable_pool {
    struct mptable_pool_hdr hdr;
    uint8_t data[MPTABLE_POOL_SIZE_PER_NODE];
};

struct buddy_table_off {
    mptable_off_t mt, st_hint;
} __packed;

int addr_in_log(unsigned long addr) {
	struct log_layer* layer = LOG(bonsai);
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		if ((unsigned long)layer->pmem_addr[node] <= addr &&
			addr <= ((unsigned long)layer->pmem_addr[node] + LOG_REGION_SIZE))
			return 1;
	}

	return 0;
}

int addr_in_pnode(unsigned long addr) {
	struct data_layer* layer = DATA(bonsai);
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		if ((layer->region[node].start <= addr) &&
			(addr <= layer->region[node].start + DATA_REGION_SIZE))
			return 1;
	}

	return 0;
}

static inline void slot_set_cnt(uint8_t *slots, int cnt) {
    slots[0] = cnt;
}

static inline int slot_get_cnt(const uint8_t *slots) {
    return slots[0];
}

static inline uint8_t slot_get(const uint8_t *slots, int pos) {
    return ((slots + 1)[pos / 2u] >> (4 * (pos % 2u))) & 0xf;
}

static inline void slot_set(uint8_t *slots, int pos, uint8_t idx) {
    unsigned int off = 4 * (pos % 2u);
    uint8_t *start = &(slots + 1)[pos / 2u];
    *start = (*start & ~(0xf << off)) + (idx << off);
}

static inline uint8_t slot_find_unused_idx(const uint8_t *slots, int slave) {
    uint8_t used[MPTABLE_MAX_KEYN] = { 0 };
    int from, to, i;

    from = MT_FROM_POS;
    to = slot_get_cnt(slots);

    if (slave) {
        from = ST_FROM_POS;
    } else if (to > MT_TO_POS) {
        to = MT_TO_POS;
    }

    for (i = from; i < to; i++) {
        used[slot_get(slots, i)] = 1;
    }

    for (i = 0; i < MPTABLE_MAX_KEYN; i++) {
        if (!used[i]) {
            return i;
        }
    }

    return NOT_FOUND;
}

static inline void slot_insert(uint8_t *slots, int pos, uint8_t idx) {
    int i, cnt = slot_get_cnt(slots);
    for (i = cnt; i >= pos + 1; i--) {
        slot_set(slots, i, slot_get(slots, i - 1));
    }
    slot_set(slots, pos, idx);
    slot_set_cnt(slots, cnt + 1);
}

static inline void slot_remove(uint8_t *slots, int pos) {
    int i, cnt = slot_get_cnt(slots);
    for (i = pos; i < cnt - 1; i++) {
        slot_set(slots, i, slot_get(slots, i + 1));
    }
    slot_set_cnt(slots, cnt - 1);
}

static inline int slot_is_full(const uint8_t *slots) {
    return slot_get_cnt(slots) == ST_TO_POS;
}

static inline int slot_is_half_full(const uint8_t *slots) {
    return slot_get_cnt(slots) >= MT_TO_POS;
}

static inline int slot_is_empty(const uint8_t *slots) {
    return slot_get_cnt(slots) == 0;
}

static inline mptable_off_t desc_addr2off(int node, struct mptable_desc *addr) {
    return (void *) addr - (void *) SHIM(bonsai)->pools[node]->data;
}

static inline struct mptable_desc *desc_off2addr(int node, mptable_off_t off) {
    return (struct mptable_desc *) ((void *) SHIM(bonsai)->pools[node]->data + off);
}

static inline mptable_off_t mptable_addr2off(int node, struct mptable *addr) {
    const unsigned long desc_size = sizeof(struct mptable_desc);
    return desc_addr2off(node, addr);
}

static inline struct mptable *mptable_off2addr(int node, mptable_off_t off) {
    const unsigned long desc_size = sizeof(struct mptable_desc);
    return (struct mptable*) ((unsigned long) desc_off2addr(node, off));
}

static struct mptable *buddy_table_alloc(int node) {
    struct shim_layer *s_layer = SHIM(bonsai);
    struct mptable_pool *pool = s_layer->pools[node];
    struct mptable_desc *desc;
    const unsigned long desc_size = sizeof(struct mptable_desc); 

    spin_lock(&pool->hdr.lock);
    assert(pool->hdr.free);
    desc = pool->hdr.free;
    pool->hdr.free = desc_off2addr(node, desc->next);
    if (likely(pool->hdr.free)) {
        pool->hdr.free->prev = 0;
    }

    if (likely(pool->hdr.inuse)) {
        desc->next = desc_addr2off(node, pool->hdr.inuse);
        pool->hdr.inuse->prev = desc_addr2off(node, desc);
    } else {
        desc->next = 0;
    }
    pool->hdr.inuse = desc;
    spin_unlock(&pool->hdr.lock);  

    return (struct mptable*) desc + desc_size;
}

static void buddy_table_dealloc(int node, struct mptable *mt) {
    struct shim_layer *s_layer = SHIM(bonsai);
    struct mptable_pool *pool = s_layer->pools[node];
    const unsigned long desc_size = sizeof(struct mptable_desc); 
    struct mptable_desc *prev, *victim = (struct mptable*) ((uint64_t)mt - desc_size);

    spin_lock(&pool->hdr.lock);

    prev = victim->prev;
    if (prev) {
        prev->next = node, victim->next;
    } else {
        pool->hdr.inuse = desc_addr2off(node, victim->next);
    }

    victim->prev = 0;

    if (likely(pool->hdr.free)) {
        victim->next = desc_addr2off(node, pool->hdr.free);
    } else {
        victim->next = 0;
    }
    pool->hdr.free = victim;
    spin_unlock(&pool->hdr.lock);  
}

static inline void mt_header_init(struct mptable *mt) {
    pkey_t __key;
#ifdef LONG_KEY
    char max_key[KEY_MAX_LEN];
    memset(max_key, -1, sizeof(max_key));
    __key = make_key(max_key, KEY_MAX_LEN);
#else
    __key = ULONG_MAX;
#endif
    slot_set_cnt(mt->slots, 0);
    seqcount_init(&mt->seq);
    seqcount_init(&mt->slots_seq);
    mt->max = __key;
    mt->slave = 0;
    mt->fence = 0;
    mt->next = 0;
    spin_lock_init(&mt->lock);
}

static inline void signature_init(struct mptable *t) {
    int i;
    for (i = 0; i < MPTABLE_MAX_KEYN; i++) {
        t->signatures[i] = SIGNATURE_NOENT;
    }
}

static inline void st_init(struct mptable *st) {
    signature_init(st);
}

static inline void mt_init(struct mptable *mt) {
    mt_header_init(mt);
    signature_init(mt);
}

static struct mptable *mptable_create(int node) {
    struct mptable *m = buddy_table_alloc(node);
    mt_init(&m[0]);
    st_init(&m[1]);
    m[0].slave = mptable_addr2off(node, &m[1]);
    return &m[0];
}

static inline void mptable_lock(struct mptable *mptable) {
    spin_lock(&mptable->lock);
}

static inline void mptable_unlock(struct mptable *mptable) {
    spin_unlock(&mptable->lock);
}

static inline void mptable_free(struct mptable *mt) {
    /* TODO: Implement it! */
}

static inline int mptable_lock_correct(int node, struct mptable **mt, pkey_t key) {
    struct mptable *target;
    mptable_lock(*mt);
    if (unlikely((*mt)->fence == ULONG_MAX)) {
        /* It's deleted. */
        mptable_unlock(*mt);
        return -EAGAIN;
    }
    while (pkey_compare(key, (*mt)->max) >= 0) {
        target = mptable_off2addr(node, (*mt)->next);
        mptable_lock(target);
        mptable_unlock(*mt);
        *mt = target;
    }
    return 0;
}

static inline void mptable_set_slots_unlocked(struct mptable *mt, uint8_t *slots) {
    memcpy(mt->slots, slots, SLOTS_SIZE);
}

static inline void mptable_set_slots(struct mptable *mt, uint8_t *slots) {
    write_seqcount_begin(&mt->slots_seq);
    mptable_set_slots_unlocked(mt, slots);
    write_seqcount_end(&mt->slots_seq);
}

static inline void mptable_get_slots_optimistic(struct mptable *mt, uint8_t *slots) {
    unsigned int seq;
    do {
        seq = read_seqcount_begin(&mt->slots_seq);
        memcpy(slots, mt->slots, SLOTS_SIZE);
    } while (read_seqcount_retry(&mt->slots_seq, seq));
}

static inline struct mptable *table_of_pos(struct mptable *mt, struct mptable *st, int pos) {
    return pos < MT_TO_POS ? mt : st;
}

static inline struct mptable *table_of_key(struct mptable *mt, struct mptable *st, const uint8_t *slots, pkey_t key) {
    return !slot_is_half_full(slots) || pkey_compare(key, mt->entries[slot_get(slots, MID_POS)].k) <= 0 ? mt : st;
}

static inline void mptable_get_pos_range(struct mptable *mt, struct mptable *target, const uint8_t *slots,
                                         int *from, int *to) {
    if (target == mt) {
        *from = MT_FROM_POS;
        *to = MT_TO_POS;
    } else {
        *from = ST_FROM_POS;
        *to = ST_TO_POS;
    }
    *to = min(*to, slot_get_cnt(slots));
}

/*
 * Find the last entry <= key, return its pos in slot array.
 */
static inline int mptable_find_upperbound(struct mptable *mt, struct mptable *st, const uint8_t *slots, pkey_t key) {
    struct mptable *target = table_of_key(mt, st, slots, key);
    int from, to, i;
    uint8_t idx;
    mptable_get_pos_range(mt, target, slots, &from, &to);
    for (i = from; i < to; i++) {
        idx = slot_get(slots, i);
        if (pkey_compare(target->entries[idx].k, key) > 0) {
            break;
        }
    }
    return i - 1;
}

static inline int mptable_find_equal(struct mptable *mt, struct mptable *st, uint8_t *slots, pkey_t key) {
    struct mptable *target = table_of_key(mt, st, slots, key);
    int from, to, i;
    uint8_t idx;
    mptable_get_pos_range(mt, target, slots, &from, &to);
    for (i = from; i < to; i++) {
        idx = slot_get(slots, i);
        if (target->signatures[idx] == pkey_get_signature(key)) {
            if (!pkey_compare(target->entries[idx].k, key)) {
                return idx;
            }
        }
    }
    return NOT_FOUND;
}

typedef enum {
    SEEK_ST = 4,    // 0b100
    SEEK_MT_W = 1,  // 0b001
    SEEK_ST_W = 6,  // 0b110
} seek_opt_t;

#define mptable_prefetch(prefetcher, t) do { \
        void *__UNIQUE_ID(prefetch_ptr);    \
        for (__UNIQUE_ID(prefetch_ptr) = (void *) (t); \
             __UNIQUE_ID(prefetch_ptr) < (void *) (t) + sizeof(struct mptable); \
             __UNIQUE_ID(prefetch_ptr) += CACHELINE_SIZE) {    \
            prefetcher(__UNIQUE_ID(prefetch_ptr));  \
        }                                     \
    } while (0)

/*
 * Find the key's corresponding mptable. Prefetch the MT and ST
 * beforehand.
 */
static inline struct mptable *mptable_seek(int node, pkey_t key, seek_opt_t seek_opt) {
    struct index_layer *i_layer = INDEX(bonsai);
    struct mptable *mt, *st_hint;
    struct buddy_table_off bt;
    pkey_t __key;
    uint16_t len;

    __key = resolve_key(&key, &len);
    *(void **) &bt = i_layer->lookup(i_layer->index_struct[node], __key, len);

    mt = mptable_off2addr(node, bt.mt);
    st_hint = mptable_off2addr(node, bt.st_hint);

    if (seek_opt & SEEK_MT_W) {
        mptable_prefetch(cache_prefetchw_high, mt);
    } else {
        mptable_prefetch(cache_prefetchr_high, mt);
    }

    if (seek_opt & SEEK_ST_W) {
        mptable_prefetch(cache_prefetchw_high, st_hint);
    } else if (seek_opt & SEEK_ST) {
        mptable_prefetch(cache_prefetchr_high, st_hint);
    }

    return mt;
}

static void mptable_dump(int node, struct mptable *mt) {
    struct mptable *st;
    kvpair_t *ent;
    uint8_t idx;
    int i, cnt;

    st = mptable_off2addr(node, mt->slave);

    cnt = slot_get_cnt(mt->slots);
    printf("Buddy table %p->%p->%p nr:%u max:%lu fence:%lu\n",
           mt, st, mptable_off2addr(node, mt->next), cnt, mt->max, mt->fence);
    printf("MT:\n");
    for (i = 0; i < cnt && i < MPTABLE_MAX_KEYN; i++) {
        idx = slot_get(mt->slots, i);
        ent = &mt->entries[idx];
        printf("MT[%lu{%d}]=%p\n", ent->k, mt->signatures[idx], ent->v);
    }
    printf("ST:\n");
    for (i = 0; i + ST_FROM_POS < cnt && i < MPTABLE_MAX_KEYN; i++) {
        idx = slot_get(mt->slots, i + ST_FROM_POS);
        ent = &st->entries[idx];
        printf("ST[%lu{%d}]=%p\n", ent->k, st->signatures[idx], ent->v);
    }
}

static void mptable_check(int node, struct mptable *mt) {
    struct mptable *st;
    uint8_t i1, i2;
    int i, cnt;

    st = mptable_off2addr(node, mt->slave);

    cnt = slot_get_cnt(mt->slots);
    for (i = 1; i < cnt && i < MPTABLE_MAX_KEYN; i++) {
        i1 = slot_get(mt->slots, i);
        i2 = slot_get(mt->slots, i - 1);
        assert(pkey_compare(mt->entries[i1].k, mt->entries[i2].k) >= 0);
    }
    for (i = 1; i + ST_FROM_POS < cnt && i < MPTABLE_MAX_KEYN; i++) {
        i1 = slot_get(mt->slots, i + ST_FROM_POS);
        i2 = slot_get(mt->slots, i - 1 + ST_FROM_POS);
        assert(pkey_compare(st->entries[i1].k, st->entries[i2].k) >= 0);
    }
}

static void mptable_split(int node, struct mptable **mt_ptr, uint8_t *slots, pkey_t key) {
    struct mptable *bdt = buddy_table_alloc(node), *mt = *mt_ptr;
    struct mptable *st = mptable_off2addr(node, mt->slave);
    struct index_layer *i_layer = INDEX(bonsai);
    struct buddy_table_off bt;
    pkey_t fence;
    uint8_t idx;
    pkey_t __key;
    uint16_t len;
    int i;

    st_init(&bdt[0]);
    st_init(&bdt[1]);

    mt_header_init(st);
    slot_set_cnt(slots, MPTABLE_MAX_KEYN);
    slot_set_cnt(st->slots, MPTABLE_MAX_KEYN);
    st->slave = mptable_addr2off(node, &bdt[1]);
    for (i = 0; i < MPTABLE_MAX_KEYN; i++) {
        idx = slot_get(slots, i + ST_FROM_POS);
        slot_set(st->slots, i, idx);
    }
    st->next = mt->next;

    fence = st->entries[slot_get(st->slots, MT_FROM_POS)].k;
    st->fence = fence;
    st->max = mt->max;

    mptable_lock(st);

    write_seqcount_begin(&mt->seq);
    mptable_set_slots_unlocked(mt, slots);
    mt->max = fence;
    mt->slave = mptable_addr2off(node, &bdt[0]);
    mt->next = mptable_addr2off(node, st);
    write_seqcount_end(&mt->seq);

    bt.mt = mptable_addr2off(node, st);
    bt.st_hint = mptable_addr2off(node, &bdt[1]);

    __key = resolve_key(&fence, &len);
    i_layer->insert(i_layer->index_struct[node], __key, len,*(void **) &bt);

    bt.mt = mptable_addr2off(node, mt);
    bt.st_hint = mptable_addr2off(node, &bdt[0]);

    __key = resolve_key(&mt->fence, &len);
    i_layer->update(i_layer->index_struct[node], __key, len, *(void **) &bt);

    if (key >= fence) {
        mptable_unlock(mt);
        *mt_ptr = st;
    } else {
        mptable_unlock(st);
    }
}

static void init_shim_pool(struct mptable_pool* pool) {
    int num_table;
    size_t start, off;
    const unsigned long table_size = sizeof(struct mptable) + sizeof(struct mptable_desc);
    const unsigned long padding = 128;
    struct mptable_desc* desc;
    int i;

    /* 2G / 320 B ... 128B */
    num_table = MPTABLE_POOL_SIZE_PER_NODE / table_size;
    start = pool->data;
    off = padding;

    for (i = 0; i < num_table; i++) {
        desc = (struct mptable_desc*) (start + off);
        desc->off = off;
        desc->prev = (i == 0) ? 0 : off - table_size;
        desc->next = (i == num_table - 1) ? 0 : off + table_size;
        off += table_size;
    }

    spin_lock_init(&pool->hdr.lock);
    pool->hdr.free = (struct mptable_desc*) (start + padding);
    pool->hdr.inuse = NULL;
}

int shim_layer_init(struct shim_layer *s_layer) {
    struct index_layer *i_layer = INDEX(bonsai);
    struct mptable *mt;
    struct buddy_table_off bt;
    int node;
    pkey_t key, __key;
    int len;

    for (node = 0; node < NUM_SOCKET; node++) {
        s_layer->pools[node] = numa_alloc_onnode(sizeof(struct mptable_pool), node);
        init_shim_pool(s_layer->pools[node]);
        mt = mptable_create(node);
        bt.mt = mptable_addr2off(node, mt);
        bt.st_hint = mt->slave;

#ifndef LONG_KEY
    key = 0;
    __key = &key;
    len = 8;
#else
    char* ch = "0";
    len = 1;
    __key = (pkey_t) ch;
#endif
        i_layer->insert(i_layer->index_struct[node], __key, len, *(void **) &bt);
    }
    return 0;
}

int shim_upsert(int node, pkey_t key, pval_t *val) {
    struct mptable *mt, *st, *target;
    uint8_t slots[SLOTS_SIZE], *sig;
    uint8_t src_idx, dst_idx;
    int i, overlap, ret;
    kvpair_t *ent;

relookup:
    mt = mptable_seek(node, key, SEEK_MT_W | SEEK_ST_W);

    ret = mptable_lock_correct(node, &mt, key);
    if (unlikely(ret == -EAGAIN)) {
        /* The mptable has been deleted. */
        goto relookup;
    }

    memcpy(slots, mt->slots, SLOTS_SIZE);

    if (unlikely(slot_is_full(slots))) {
        mptable_split(node, &mt, slots, key);
        memcpy(slots, mt->slots, SLOTS_SIZE);
    }

    st = mptable_off2addr(node, mt->slave);

    i = mptable_find_upperbound(mt, st, slots, key);

    if (i != NOT_FOUND) {
        ent = &table_of_pos(mt, st, i)->entries[slot_get(slots, i)];
        if (unlikely(key == ent->k)) {
            /*
             * The linearization point of update operation. We
             * do not need to get seq counter here. If there're
             * some readers holding the stale value ptr, they can
             * still access the correct data inside grace period.
             */
            ent->v = val;
            ret = -EEXIST;
            goto out;
        }
    }
    i++;

    target = table_of_pos(mt, st, i);
    if (target == st || !slot_is_half_full(slots)) {
        dst_idx = slot_find_unused_idx(slots, target == st);
        slot_insert(slots, i, dst_idx);

        // TODO: generation
        overlap = target->signatures[dst_idx] != SIGNATURE_NOENT;

        if (overlap) {
            /* The linearization point of overlapping insert operation without exchange */
            write_seqcount_begin(&mt->seq);
        }

        target->signatures[dst_idx] = pkey_get_signature(key);
        target->entries[dst_idx] = (kvpair_t) { key, val };

        /* The linearization point of non-overlapping insert operation without exchange */
        (overlap ? mptable_set_slots_unlocked : mptable_set_slots)(mt, slots);

        if (overlap) {
            write_seqcount_end(&mt->seq);
        }
    } else {
        dst_idx = slot_find_unused_idx(slots, 1);
        src_idx = slot_get(slots, MID_POS);
        ent = &mt->entries[src_idx];
        sig = &mt->signatures[src_idx];

        st->entries[dst_idx] = *ent;
        st->signatures[dst_idx] = *sig;

        slot_set(slots, MID_POS, dst_idx);
        slot_insert(slots, i, src_idx);

        /* The linearization point of insert operation with exchange */
        write_seqcount_begin(&mt->seq);

        *sig = pkey_get_signature(key);
        ent->k = key;
        ent->v = val;

        mptable_set_slots_unlocked(mt, slots);

        write_seqcount_end(&mt->seq);
    }

out:
    mptable_unlock(mt);
    return ret;
}

static void mptable_detach(int node, struct mptable *mt) {
    struct mptable *prev;

relookup:
    prev = mptable_seek(node, pkey_prev(mt->fence), SEEK_MT_W);
    mptable_lock(prev);
    if (unlikely(mptable_off2addr(node, prev->next) != mt)) {
        mptable_unlock(prev);
        goto relookup;
    }

    write_seqcount_begin(&prev->seq);
    prev->max = mt->max;
    prev->next = mt->next;
    write_seqcount_end(&prev->seq);

    mptable_unlock(prev);

    /* Avoid insertion to this table. */
    mt->fence = ULONG_MAX;
    spin_unlock(&mt->lock);

    /* Now @mt has been isolated. Defer free it. */
    call_rcu(NULL, mptable_free, mt);
}

int shim_remove(int node, pkey_t key) {
    struct mptable *mt, *st;
    uint8_t slots[SLOTS_SIZE];
    int ret, pos;

    mt = mptable_seek(node, key, SEEK_MT_W | SEEK_ST);

    ret = mptable_lock_correct(node, &mt, key);
    if (unlikely(ret == -EAGAIN)) {
        /* The mptable has been deleted. */
        ret = -ENOENT;
        goto out_unlocked;
    }

    memcpy(slots, mt->slots, SLOTS_SIZE);

    st = mptable_off2addr(node, mt->slave);
    pos = mptable_find_equal(mt, st, slots, key);
    if (unlikely(pos == NOT_FOUND)) {
        ret = -ENOENT;
        goto out;
    }

    slot_remove(slots, pos);

    /* The linearization point of remove operation. */
    mptable_set_slots(mt, slots);

    if (slot_is_empty(slots)) {
        mptable_detach(node, mt);
        goto out_unlocked;
    }

out:
    mptable_unlock(mt);

out_unlocked:
    return ret;
}

int shim_lookup(int node, pkey_t key, pval_t *val) {
    struct mptable *mt, *st, *target;
    uint8_t slots[SLOTS_SIZE];
    pval_t *addr, tmp = 0;
    struct oplog *log;
    unsigned int seq;
    int ret, pos;

    mt = mptable_seek(node, key, SEEK_ST);

retry:
    seq = read_seqcount_begin(&mt->seq);

    /*
     * Note that mt->max is always a valid pointer to string
     * any time due to its atomic assignment and delay-free
     * strategy.
     */
    if (unlikely(pkey_compare(key, mt->max) >= 0)) {
        /*
         * Really within this node? If not, split happens. We'll
         * go to the target node. But we should guarantee that our
         * mt->max and mt->target are consistent.
         */
        target = mptable_off2addr(node, mt->next);
        if (unlikely(read_seqcount_retry(&mt->seq, seq))) {
            goto retry;
        }
        mt = target;
        goto retry;
    }

    mptable_get_slots_optimistic(mt, slots);

    st = mptable_off2addr(node, ACCESS_ONCE(mt->slave));
    if (unlikely(read_seqcount_retry(&mt->seq, seq))) {
        /*
         * We should guarantee that @target is still consistent
         * with the slot array. Note that the consistent here do
         * not mean that it's unchanged, but all the positions
         * included in the slot array are valid addresses.
         */
        goto retry;
    }

    pos = mptable_find_equal(mt, st, slots, key);
    if (unlikely(pos == NOT_FOUND)) {
        ret = -ENOENT;
        goto done;
    }

    /*
     * Dereferencing maybe-invalid addresses in log region or
     * pnode region is OK.
     */
    addr = table_of_pos(mt, st, pos)->entries[pos].v;
    ret = 0;
    if (addr_in_log((unsigned long) addr)) {
        log = OPLOG(addr);
        switch (log->o_type) {
            case OP_INSERT:
                tmp = log->o_kv.v;
                break;
            case OP_REMOVE:
                ret = -ENOENT;
                break;
            default:
                goto retry;
        }
    } else if (addr_in_pnode((unsigned long) addr)) {
        tmp = *addr;
    }

done:
    if (unlikely(read_seqcount_retry(&mt->seq, seq))) {
        goto retry;
    }

    if (likely(!ret)) {
        *val = tmp;
    }
    return ret;
}

void shim_dump(int node) {
    struct mptable *mt = mptable_seek(node, 0, SEEK_ST);
    while (1) {
        mptable_dump(node, mt);
        if (!mt->next) {
            break;
        }
        mt = mptable_off2addr(node, mt->next);
    }
}
