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
#include "config.h"
#include "region.h"

#define NUM_OPLOG_PER_CPU       (LOG_REGION_SIZE / NUM_CPU / sizeof(struct oplog))

typedef enum {
	OP_NOP = 0,
    OP_INSERT = 2,
	OP_REMOVE = 4,
} optype_t;

typedef enum {
	TX_OP = 0,
	TX_COMMIT = 8,
	TX_ROLLBACK = 16,
} txop_t;

typedef struct {
    int flip;
} log_state_t;

typedef uint32_t logid_t;

struct log_layer {
	unsigned int nflush; /* how many flushes */
	atomic_t exit; /* thread exit */
    atomic_t force_flush; /* force flush all logs */

    atomic_t epoch_passed;
	atomic_t checkpoint;

    struct {
        atomic_t cnt;
        char padding[CACHELINE_SIZE];
    } nlogs[NUM_CPU];

	struct log_region_desc *desc;
    struct dimm_log_region *dimm_regions[NUM_DIMM];
    int dimm_region_fd[NUM_DIMM];

    log_state_t lst;
};

/* 48B/32B */
struct oplog {
    pentry_t o_kv;    /* key-value pair */
	__le64   o_stamp; /* time stamp */
	__le64   o_type;  /* OP_INSERT or OP_REMOVE */
} __packed;

struct cpu_log_region_meta {
    __le32 start, end;
} __packed ____cacheline_aligned2;

struct cpu_log_region {
	struct cpu_log_region_meta meta;
    struct oplog logs[NUM_OPLOG_PER_CPU];
} __packed;

struct dimm_log_region {
    struct cpu_log_region regions[NUM_CPU_PER_DIMM];
} __packed;

struct cpu_log_region_desc {
    struct cpu_log_region *region;
    pthread_mutex_t *dimm_lock;

    uint32_t start, end;
    struct oplog *lcb;
    size_t lcb_size;

    seqcount_t seq;

    enum {
        WBS_ENABLE,
        WBS_DELAY,
        WBS_REQUEST
    } wb_state;
    void *wb_new_buf;
    int wb_done;
} ____cacheline_aligned2;

struct log_region_desc {
    struct cpu_log_region_desc descs[NUM_CPU];
};

struct oplog *oplog_get(logid_t logid);

struct pnode;
struct mptable;
struct log_layer;

extern void oplog_snapshot_lst(log_state_t *lst);

extern logid_t oplog_insert(log_state_t *lst, pkey_t key, pval_t val, optype_t op, txop_t txop, int cpu);

extern void oplog_flush();

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

int log_layer_init(struct log_layer* layer);
void log_layer_deinit(struct log_layer* layer);

#ifdef __cplusplus
}
#endif

#endif
