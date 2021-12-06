#ifndef __THREAD_H
#define __THREAD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

#include "numa_config.h"
#include "list.h"

#define NUM_PFLUSH_THREAD	4

struct log_layer;
struct log_blk;

enum {
	WORKER_SLEEP = 0,
	WORKER_RUNNING = NUM_PFLUSH_THREAD,
};

struct merge_work {
	unsigned int id;
	unsigned int count;
	struct log_layer* layer;
	struct log_blk* first_blks[NUM_CPU/NUM_PFLUSH_THREAD];
	struct log_blk* last_blks[NUM_CPU/NUM_PFLUSH_THREAD];
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

static inline int get_tid() {
	return 0;
}

extern __thread struct thread_info* __this;

extern int bonsai_thread_init(struct bonsai* bonsai);
extern int bonsai_thread_exit(struct bonsai* bonsai);

#ifdef __cplusplus
}
#endif

#endif
