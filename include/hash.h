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

#define htable_ent_num(table) ()

extern struct htable* htable_alloc(int num);
extern int htable_init(struct htable* table);
extern int htable_insert(struct htable* table, hkey_t key, hval_t val, hnode_t* hnode);
extern int htable_remove(struct htable* table, hkey_t key);
extern int htable_lookup(struct htable* table, hkey_t key, hval_t* result);
extern int htable_update(struct htable* table, hkey_t key, hval_t val);

#endif
/*hash.h*/
