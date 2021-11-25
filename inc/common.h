#ifndef COMMON_H
#define COMMON_H

typedef uint64_t pkey_t;
typedef uint64_t pval_t;

typedef struct pentry {
    pkey_t key;
    pval_t value;
} pentry_t;

#define ERR_THREAD		100
#define ERR_NOMEM		101
#define ERR_NOENT		102

#endif
