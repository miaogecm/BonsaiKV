#ifndef _HASH_H
#define _HASH_H

#include <stdint.h>

typedef uint64_t hkey_t;

typedef struct {
	pkey_t 		e_key;
    void* 		e_addr;
}mtable_ent_t;

typedef mtable_ent_t hval_t;

typedef struct {
    hkey_t key;
    hval_t val;
}hnode_t;

struct htable {
    
};

static inline uint64_t hash(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

#define htable_ent_num(table) ()

extern struct htable* htable_alloc(int num);
extern int htable_init(struct htable* table);
extern int htable_insert(struct htable* table, hkey_t key, hval_t val, hnode_t* hnode);
extern int htable_remove(struct htable* table, hkey_t key);
extern int htable_lookup(struct htable* table, hkey_t key, hval_t* result);
extern int htable_update(struct htable* table, hkey_t key, hval_t val);

#endif
/*hash.h*/