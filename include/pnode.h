#ifndef __PNODE_H
#define __PNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "arch.h"
#include "rwlock.h"
#include "list.h"
#include "common.h"
#include "hwconfig.h"

#define PNODE_FANOUT            56

#define PNODE_FULL_VALIDMAP     ((1ul << PNODE_FANOUT) - 1)

#define PNOID_NONE              (-1u)

typedef uint32_t pnoid_t;

typedef struct pnode {
    /* cacheline 0 */
    __le64        validmap;
    __le8         fgprt[PNODE_FANOUT];

    /* cacheline [1, 28] */
    pentry_t      ents[PNODE_FANOUT];

    /* cacheline 29 */
    rwlock_t     *lock;
    pkey_t        anchor;
    pnoid_t       prev, next;
} pnode_t;

typedef struct {
    enum {
        PBO_NONE = 0,
        PBO_INSERT,
        PBO_REMOVE
    }      type;
    pkey_t key;
    pval_t val;
    int    done;
} pbatch_op_t;

static inline int pnode_color(pnode_t *pnode) {

}

void pnode_split_and_recolor(pnode_t **pnode, pnode_t **sibling, pkey_t *cut, int lc, int rc);
void pnode_run_batch(pnode_t *pnode, pbatch_op_t *ops);

static inline void pnode_split(pnode_t **pnode, pnode_t **sibling, pkey_t *cut) {
    pnode_split_and_recolor(pnode, sibling, cut, pnode_color(*pnode), pnode_color(*sibling));
}

static inline void pnode_recolor(pnode_t **pnode, int c) {
    pnode_split_and_recolor(pnode, NULL, NULL, c, c);
}

static inline pnode_t *pnode_get(pnoid_t id) {
    return NULL;
}

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
