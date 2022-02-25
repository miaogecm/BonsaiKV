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

extern struct bonsai_info* bonsai;

#define MID_POS         (MPTABLE_MAX_KEYN - 1)
/* [from, to) */
#define MT_FROM_POS     0
#define MT_TO_POS       MPTABLE_MAX_KEYN
/* [from, to) */
#define ST_FROM_POS     MT_TO_POS
#define ST_TO_POS       BDTABLE_MAX_KEYN

#define NOT_FOUND           (-1)
#define SIGNATURE_NOENT     0xff

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

static inline uint8_t slot_find_unused_idx(const uint8_t *slots, int slave) {

}

static inline uint8_t slot_get(const uint8_t *slots, int pos) {

}

static inline void slot_set(uint8_t *slots, int pos, uint8_t idx) {

}

static inline void slot_insert(uint8_t *slots, int pos, uint8_t idx) {

}

static inline void slot_remove(uint8_t *slots, int pos) {

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

static inline mptable_off_t mptable_addr2off(struct mptable *addr) {

}

static inline struct mptable *mptable_off2addr(mptable_off_t off) {

}

static inline void mt_header_init(struct mptable *mt) {
    slot_set_cnt(mt->slots, 0);
    seqcount_init(&mt->seq);
    seqcount_init(&mt->slots_seq);
    mt->max = ULONG_MAX;
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

static void *mptable_create() {
    struct mptable *m = memalign(CACHELINE_SIZE, 2 * sizeof(*m));
    mt_init(&m[0]);
    st_init(&m[1]);
    m[0].slave = mptable_addr2off(&m[1]);
    return &m[0];
}

static inline void mptable_lock(struct mptable *mptable) {
    spin_lock(&mptable->lock);
}

static inline void mptable_unlock(struct mptable *mptable) {
    spin_unlock(&mptable->lock);
}

static inline void mptable_free(struct mptable *mt) {

}

static inline int mptable_lock_correct(struct mptable **mt, pkey_t key) {
    struct mptable *target;
    mptable_lock(*mt);
    if (unlikely((*mt)->fence == ULONG_MAX)) {
        /* It's deleted. */
        mptable_unlock(*mt);
        return -EAGAIN;
    }
    while (pkey_compare(key, (*mt)->max) >= 0) {
        target = mptable_off2addr((*mt)->next);
        mptable_lock(target);
        mptable_unlock(*mt);
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
        if (pkey_compare(mt->entries[idx].k, key) > 0) {
            break;
        }
    }
    return i == from ? NOT_FOUND : i - 1;
}

static inline int mptable_find_equal(struct mptable *mt, struct mptable *st, uint8_t *slots, pkey_t key) {
    struct mptable *target = table_of_key(mt, st, slots, key);
    int from, to, i;
    uint8_t idx;
    mptable_get_pos_range(mt, target, slots, &from, &to);
    for (i = from; i < to; i++) {
        idx = slot_get(slots, i);
        if (mt->signatures[idx] == pkey_get_signature(key)) {
            if (!pkey_compare(mt->entries[idx].k, key)) {
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

/*
 * Find the key's corresponding mptable. Prefetch the MT and ST
 * beforehand.
 */
static inline struct mptable *mptable_seek(pkey_t key, seek_opt_t seek_opt) {

}

static void mptable_split(struct mptable *mt, uint8_t *slots) {
    struct mptable *bdt = memalign(CACHELINE_SIZE, 2 * sizeof(*bdt));
    struct mptable *st = mptable_off2addr(mt->slave);
    struct index_layer *i_layer = INDEX(bonsai);
    struct buddy_table_off bt;
    pkey_t fence;
    uint8_t idx;
    int i;

    st_init(&bdt[0]);
    st_init(&bdt[1]);

    mt_header_init(st);
    slot_set_cnt(slots, MPTABLE_MAX_KEYN);
    st->slave = mptable_addr2off(&bdt[1]);
    for (i = 0; i < MPTABLE_MAX_KEYN; i++) {
        idx = slot_get(slots, i + ST_FROM_POS);
        slot_set(st->slots, i, idx);
    }
    st->next = mt->next;

    fence = st->entries[slot_get(st->slots, MT_FROM_POS)].k;
    st->fence = fence;
    st->max = mt->max;

    write_seqcount_begin(&mt->seq);
    mptable_set_slots_unlocked(mt, slots);
    mt->max = fence;
    mt->slave = mptable_addr2off(&bdt[0]);
    mt->next = mptable_addr2off(st);
    write_seqcount_end(&mt->seq);

    mptable_unlock(mt);

    // TODO: Optimize this

    bt.mt = mptable_addr2off(st);
    bt.st_hint = mptable_addr2off(&bdt[1]);
    i_layer->insert(i_layer->index_struct, fence, *(void **) &bt);

    bt.mt = mptable_addr2off(mt);
    bt.st_hint = mptable_addr2off(&bdt[0]);
    i_layer->update(i_layer->index_struct, bdt[0].fence, *(void **) &bt);
}

int shim_upsert(pkey_t key, pval_t *val) {
    struct mptable *mt, *st, *target;
    uint8_t slots[SLOTS_SIZE];
    uint8_t src_idx, dst_idx;
    int i, overlap, ret;
    kvpair_t *ent;

relookup:
    mt = mptable_seek(key, SEEK_MT_W | SEEK_ST_W);

retry:
    ret = mptable_lock_correct(&mt, key);
    if (unlikely(ret == -EAGAIN)) {
        /* The mptable has been deleted. */
        goto relookup;
    }

    memcpy(slots, mt->slots, SLOTS_SIZE);

    if (unlikely(slot_is_full(slots))) {
        mptable_split(mt, slots);
        goto retry;
    }

    st = mptable_off2addr(mt->slave);

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

        st->entries[dst_idx] = *ent;

        slot_set(slots, MID_POS, dst_idx);
        slot_insert(slots, i, src_idx);

        /* The linearization point of insert operation with exchange */
        write_seqcount_begin(&mt->seq);

        ent->k = key;
        ent->v = val;

        mptable_set_slots_unlocked(mt, slots);

        write_seqcount_end(&mt->seq);
    }

out:
    mptable_unlock(mt);
}

static void mptable_detach(struct mptable *mt) {
    struct mptable *prev;

relookup:
    prev = mptable_seek(pkey_prev(mt->fence), SEEK_MT_W);
    mptable_lock(prev);
    if (unlikely(mptable_off2addr(prev->next) != mt)) {
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

int shim_remove(pkey_t key) {
    struct mptable *mt, *st;
    uint8_t slots[SLOTS_SIZE];
    int ret = 0, pos;

    mt = mptable_seek(key, SEEK_MT_W | SEEK_ST);

    ret = mptable_lock_correct(&mt, key);
    if (unlikely(ret == -EAGAIN)) {
        /* The mptable has been deleted. */
        ret = -ENOENT;
        goto out_unlocked;
    }

    memcpy(slots, mt->slots, SLOTS_SIZE);

    st = mptable_off2addr(mt->slave);
    pos = mptable_find_equal(mt, st, slots, key);
    if (unlikely(pos == NOT_FOUND)) {
        ret = -ENOENT;
        goto out;
    }

    slot_remove(slots, pos);

    /* The linearization point of remove operation. */
    mptable_set_slots(mt, slots);

    if (slot_is_empty(slots)) {
        mptable_detach(mt);
        goto out_unlocked;
    }

out:
    mptable_unlock(mt);

out_unlocked:
    return ret;
}

int shim_lookup(pkey_t key, pval_t *val) {
    struct mptable *mt, *st, *target;
    uint8_t slots[SLOTS_SIZE];
    struct oplog *log;
    pval_t *addr, tmp;
    unsigned int seq;
    int ret, pos;

    mt = mptable_seek(key, SEEK_ST);

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
        target = mptable_off2addr(mt->next);
        if (unlikely(read_seqcount_retry(&mt->seq, seq))) {
            goto retry;
        }
        mt = target;
        goto retry;
    }

    mptable_get_slots_optimistic(mt, slots);

    st = mptable_off2addr(ACCESS_ONCE(mt->slave));
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
