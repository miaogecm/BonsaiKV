#ifndef COMMON_H
#define COMMON_H

typedef uint64_t pkey_t;
typedef uint64_t pval_t;

typedef struct pentry {
    pkey_t key;
    pval_t value;
} pentry_t;

#define GET_ENT(ptr) ((pentry_t*)ptr)
#define GET_KEY(ptr) (GET_ENT(ptr)->key)
#define GET_VALUE(ptr) (GET_ENT(ptr)->value)

#define ENOMEM		101 /* out-of memory */
#define ENOENT		102 /* no such entry */
#define EEXIST		103 /* key exist */

#endif
