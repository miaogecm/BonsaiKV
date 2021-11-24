#ifndef _COMMON_H
#define _COMMON_H

typedef uint64_t entry_key_t;
typedef struct entry_s {
    entry_key_t key;
    char* value;
}entry_t;

#define GET_NODE(ptr) ((entry_t*)ptr)
#define GET_KEY(ptr) (GET_NODE(ptr)->key)
#define GET_VALUE(ptr) (GET_NODE(ptr)->value)

#endif
/*common.h*/
