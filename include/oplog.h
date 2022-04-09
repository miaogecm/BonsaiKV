#ifndef __OPLOG_H
#define __OPLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

#include "atomic.h"
#include "list.h"
#include "common.h"
#include "arch.h"
#include "seqlock.h"

#define OPLOG_NUM_PER_CPU       (LOG_REGION_SIZE / NUM_CPU / sizeof(struct oplog))
#define NUM_CPU_PER_LOG_DIMM    (NUM_CPU / NUM_DIMM)

typedef enum {
    OP_INSERT = 0,
	OP_REMOVE = 2,
} optype_t;

typedef struct {
    int turn;
} log_state_t;

typedef uint32_t logid_t;

/* 48B/32B */
struct oplog {
    pentry_t o_kv;    /* key-value pair */
	__le64   o_stamp; /* time stamp */
	__le64   o_type;  /* OP_INSERT or OP_REMOVE */
} __packed;

struct cpu_log_region_meta {
    __le32 start, end;
} __packed;

struct cpu_log_region {
    struct cpu_log_region_meta meta;
    struct oplog logs[OPLOG_NUM_PER_CPU];
} __packed;

struct dimm_log_region {
    struct cpu_log_region regions[NUM_CPU_PER_LOG_DIMM];
} __packed;

struct cpu_log_region_desc {
    struct cpu_log_region *region;
    pthread_mutex_t *dimm_lock;

    size_t size;
    struct oplog *lcb;

    spinlock_t lock;
    seqcount_t seq;
};

struct log_region_desc {
    struct cpu_log_region_desc descs[NUM_CPU];
};

struct oplog *oplog_get(logid_t logid);

struct pnode;
struct mptable;
struct log_layer;

extern void oplog_snapshot_lst(log_state_t *lst);

extern logid_t oplog_insert(pkey_t key, pval_t val, optype_t op, int cpu);

extern void oplog_flush();

extern struct oplog* log_layer_search_key(int cpu, pkey_t key);

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

#ifdef __cplusplus
}
#endif

#endif
