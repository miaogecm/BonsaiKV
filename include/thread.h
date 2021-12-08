#ifndef __THREAD_H
#define __THREAD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdlib.h>

#include "numa_config.h"
#include "list.h"

#define NUM_PFLUSH_THREAD	4

struct log_layer;
struct oplog_blk;

enum {
	WORKER_SLEEP = 0,
	WORKER_RUNNING = NUM_PFLUSH_THREAD,
};

struct merge_work {
	unsigned int id;
	unsigned int count;
	struct log_layer* layer;
	struct oplog_blk* first_blks[NUM_CPU/NUM_PFLUSH_THREAD];
	struct oplog_blk* last_blks[NUM_CPU/NUM_PFLUSH_THREAD];
};

struct flush_work {
	unsigned int id;
	unsigned int min_index;
	unsigned int max_index;
	struct log_layer* layer;
};

typedef void (*work_func_t)(void*);

struct work_struct {
	work_func_t func;
	void* arg;
	struct list_head list;
};

struct workqueue_struct {
	struct thread_info* thread;
	struct list_head head;
};

struct thread_info {
	unsigned int 	t_id;
	unsigned int 	t_cpu;
	void* 			t_data;
	struct workqueue_struct t_wq;	
};

extern __thread struct thread_info* __this;

static inline void workqueue_add(struct workqueue_struct* wq, struct work_struct* work) {
	list_add(&work->list, &wq->head);
}

static inline void workqueue_del(struct work_struct* work) {
	list_del(&work->list);
	free(work);
}

extern int bonsai_pflushd_thread_init();
extern int bonsai_pflushd_thread_exit();

extern void bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

extern void wakeup_master();
extern void park_master();
extern void park_workers();
extern void wakeup_workers();

extern int get_tid();

#ifdef __cplusplus
}
#endif

#endif
