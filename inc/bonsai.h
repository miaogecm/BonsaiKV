#ifndef _BONSAI_H
#define _BONSAI_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <libpmemobj.h>

#include "atomic.h"
#include "cuckoo_hash.h"

typedef skiplist_t index_layer_t;

typedef cuckoo_hash_t buffer_node_t;

struct buffer_layer {
    struct oplog *oplogs;
};

struct data_layer {
	struct list_head pnodes;
};

struct bonsai {
    uint8_t 			bonsai_init;
    index_layer_t* 		ilayer;
    struct buffer_layer blayer;
    struct data_layer 	dlayer;
};

bonsai_t* bonsai_init();
int bonsai_lookup(bonsai_t* bonsai, entry_key_t key, char* result);
int bonsai_insert(bonsai_t* bonsai, entry_kry_t key, char* value);
int bonsai_remove(bonsai_t* bonsai, entry_key_t key);
int bonsai_range(bonsai_t* bonsai, entry_key_t begin_key, entry_key_t end_t, cahr** res_arr);

#define index_layer_init()              sl_init()
#define index_layer_insert(a, b, c)     sl_insert(a, b, c)
#define index_layer_remove(a, b)        sl_remove(a, b)
#define index_layer_lower_bound(a, b)   sl_lower_bound(a, b)

#define INDEX(bonsai)    (bonsai->index_layer)
#define BUFFER(bonsai)   (bonsai->buffer_layer)
#define PERSIST(bonsai)  (bonsai->persist_layer)

#endif
/*bonsai.h*/
