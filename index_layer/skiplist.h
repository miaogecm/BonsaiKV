#ifndef _SKIPLIST_H
#define _SKIPLIST_H

#include <stdint.h>

typedef uint64_t sl_key_t;

typedef struct sl_node_s {
    sl_key_t key;
    char* value;
    struct node_s* next;
}sl_node_t;

typedef struct skiplist_s {
    sl_node_t* head;
    sl_node_t* tail;
}skiplist_t;

skiplist_t* sl_init();
char* sl_lower_bound(skiplist_t* sl, sl_key_t key);
int sl_insert(skiplist_t* sl, sl_key_t key, char* value);
int sl_remove(skiplist_t* sl, sl_key_t key);

#endif
/*skiplist.h*/