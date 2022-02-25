#ifndef _SHIM_H
#define _SHIM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include "numa_config.h"
#include "hash_set.h"
#include "list.h"
#include "rwlock.h"
#include "seqlock.h"

#define BDTABLE_MAX_KEYN        24
#define MPTABLE_MAX_KEYN        (BDTABLE_MAX_KEYN / 2)
#define SLOTS_SIZE              13

typedef uint32_t mptable_off_t;

struct mptable {
	/* Header - 6 words */

    /* MT Only: */

    /* @seq protects @slots, @slave, @next, @max, and entries */
    seqcount_t seq;
    /* @slots_seq protects @slots only */
    seqcount_t slots_seq;
    spinlock_t lock;
    uint8_t slots[SLOTS_SIZE];
    mptable_off_t slave, next;
    pkey_t fence, max;
    unsigned int generation;

    /* Both: */

    /* Key signatures - 2 words */
    uint8_t signatures[MPTABLE_MAX_KEYN];

    /* Key-Value pairs - 24 words */
    kvpair_t entries[MPTABLE_MAX_KEYN];
};

struct buddy_table {
    struct mptable *mt, *st_hint;
};

int shim_upsert(pkey_t key, pval_t *val);
int shim_remove(pkey_t key);
int shim_lookup(pkey_t key, pval_t *val);

#ifdef __cplusplus
}
#endif

#endif
