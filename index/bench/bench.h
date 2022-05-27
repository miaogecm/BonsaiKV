#ifndef INDEX_BENCH_H
#define INDEX_BENCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <errno.h>


//#define STR_KEY
#ifdef STR_KEY

#define KEY_LEN             24

#define MAX_KEY             ((pkey_t) { "\x7f" })
#define MIN_KEY             ((pkey_t) { "" })

#else

#define KEY_LEN             8

#define INT2KEY(val)        (* (pkey_t *) (unsigned long []) { (val) })
#define KEY2INT(val)        (* (unsigned long *) val)

#define MAX_KEY             INT2KEY(-1UL)
#define MIN_KEY             INT2KEY(0UL)

#endif

// typedef struct {
//     char key[KEY_LEN];
// } pkey_t;

typedef uint64_t 	pval_t;

#ifdef BONSAI_DEBUG
#define bonsai_debug(fmt, args ...)	 do {fprintf(stdout, fmt, ##args);} while (0)
#else
#define bonsai_debug(fmt, args ...) do {} while(0)
#endif

extern int in_bonsai;

typedef void* (*init_func_t)(void);
typedef void (*destory_func_t)(void*);
typedef int (*insert_func_t)(void* index_struct, const void *key, size_t len, const void* value);
typedef int (*update_func_t)(void* index_struct, const void *key, size_t len, const void* value);
typedef int (*remove_func_t)(void* index_struct, const void *key, size_t len);
typedef void* (*lookup_func_t)(void* index_struct, const void *key, size_t len, const void *actual_key);
typedef int (*scan_func_t)(void* index_struct, const void *low, const void *high);

int bench(char* index_name, init_func_t init, destory_func_t destory,
          insert_func_t insert, update_func_t update, remove_func_t remove,
          lookup_func_t lookup, lookup_func_t lowerbound, scan_func_t scan);

#ifdef __cplusplus
}
#endif

#endif //INDEX_BENCH_H
