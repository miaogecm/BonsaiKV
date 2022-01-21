#ifndef __THREAD_H
#define __THREAD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdlib.h>
#include <sys/types.h>

#include "numa_config.h"
#include "list.h"

#define NUM_USER_THREAD		4

#define NUM_PFLUSH_THREAD	5
#define NUM_PFLUSH_WORKER	(NUM_PFLUSH_THREAD - 1)

#define NUM_THREAD			(NUM_USER_THREAD + NUM_PFLUSH_THREAD + 1)

struct log_layer;
struct oplog_blk;

enum {
	WORKER_SLEEP = 0,
	WORKER_RUNNING = NUM_PFLUSH_THREAD - 1,
	ALL_WAKEUP = NUM_PFLUSH_THREAD,
};

enum {
	S_RUNNING = 1,
	S_SLEEPING,
	S_INTERRUPTED,
	S_UNINIT,
	S_EXIT
};

struct merge_work {
	unsigned int count;
	struct list_head page_list;
	struct log_layer* layer;
	struct oplog_blk* first_blks[NUM_CPU/NUM_PFLUSH_WORKER];
	struct oplog_blk* last_blks[NUM_CPU/NUM_PFLUSH_WORKER];
};

struct flush_work {
	unsigned int min_index;
	unsigned int max_index;
	unsigned int curr_index;
	struct log_layer* layer;
};

typedef int (*work_func_t)(void*);
typedef void (*clean_func_t)(void*);

struct work_struct {
	work_func_t exec;
	void* exec_arg;
	clean_func_t clean;
	void* clean_arg;
	struct list_head list;
};

struct workqueue_struct {
	struct thread_info* thread;
	struct list_head head;
};

struct thread_info {
	unsigned int 			t_id;
	pid_t					t_pid;
	volatile int 			t_state;
	unsigned int 			t_cpu;
	volatile unsigned long 	t_epoch;
	void* 					t_data;
	struct workqueue_struct t_wq;
	struct list_head		list;
};

extern __thread struct thread_info* __this;

static inline void workqueue_add(struct workqueue_struct* wq, struct work_struct* work) {
	list_add(&work->list, &wq->head);
}

static inline void workqueue_del(struct work_struct* work) {
	list_del(&work->list);
	free(work);
}

extern void bonsai_self_thread_init();
extern void bonsai_self_thread_exit();

extern int bonsai_pflushd_thread_init();
extern int bonsai_pflushd_thread_exit();

extern int bonsai_user_thread_init();
extern void bonsai_user_thread_exit();

extern void wakeup_master();
extern void park_master();
extern void park_workers();
extern void wakeup_workers();

extern int get_tid();

extern void stop_the_world();

#ifdef __cplusplus
}
#endif

#endif
