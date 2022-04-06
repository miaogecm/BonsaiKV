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

typedef uint32_t pnoid_t;

#define PNOID_NULL              (-1u)

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

int pnode_node(pnoid_t pno);

static inline int pnode_color(pnoid_t pno) {
    return pnode_node(pno);
}

void pnode_split_and_recolor(pnoid_t *pnode, pnoid_t *sibling, pkey_t *cut, int lc, int rc);
void pnode_run_batch(pnoid_t pnode, pbatch_op_t *ops);

int pnode_lookup(pnoid_t pnode, pkey_t key, pval_t *val);
int is_in_pnode(pnoid_t pnode, pkey_t key);

static inline void pnode_split(pnoid_t *pnode, pnoid_t *sibling, pkey_t *cut) {
    pnode_split_and_recolor(pnode, sibling, cut, pnode_color(*pnode), pnode_color(*sibling));
}

static inline void pnode_recolor(pnoid_t *pnode, int c) {
    pnode_split_and_recolor(pnode, NULL, NULL, c, c);
}

#ifdef __cplusplus
}
#endif

#endif
/*pnode.h*/
