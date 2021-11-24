#ifndef __OPLOG_H
#define __OPLOG_H

#include "atomic.h"

typedef void (*index_func_t)(void*);

typedef enum {
	OP_INSERT = 0,
	OP_REMOVE,
} optype_t;

/*
 * operation log definition in buffer layer
 */
struct oplog {
    atomic_t time_stamp;
	optype_t optype;
	index_func_t func;
	pentry_t kv;	

    struct pnode* node;
	struct oplog* next;
};

#endif
