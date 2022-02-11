#ifndef _WRAPPER_H
#define _WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"

struct fast_fair;

extern struct fast_fair* ff_init();
extern void ff_destory(void* index_struct);
extern int ff_insert(void* index_struct, pkey_t key, void* value);
extern int ff_update(void* index_struct, pkey_t key, void* value);
extern int ff_remove(void* index_struct, pkey_t key);
extern void* ff_lookup(void* index_struct, pkey_t key);
extern int ff_scan(void* index_struct, pkey_t min, pkey_t max);

#ifdef __cplusplus
};
#endif

#endif 