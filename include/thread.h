#ifndef __THREAD_H
#define __THREAD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "config.h"
#include "list.h"
#include "common.h"

#define NUM_PFLUSH_WORKER           (NUM_PFLUSH_WORKER_PER_NODE * NUM_SOCKET)
#define NUM_PFLUSH_THREAD           (NUM_PFLUSH_WORKER + 1)

#define NUM_SMO_THREAD				1

#define CHKPT_TIME_INTERVAL		700000
#define CHKPT_NLOG_INTERVAL		10000

struct log_layer;
struct oplog_blk;

enum {
	WORKER_SLEEP = 0,
	WORKER_RUNNING = NUM_PFLUSH_THREAD - 1,
	ALL_WAKEUP = NUM_PFLUSH_THREAD,
	MASTER_SLEEP = 100,
	MASTER_WORK,
	SMO_SLEEP = 200,
	SMO_WORK,
};

enum {
	S_RUNNING = 1,
	S_SLEEPING,
	S_INTERRUPTED,
	S_UNINIT,
	S_EXIT
};

typedef int (*work_func_t)(void*);
typedef void (*clean_func_t)(void*);

struct work_struct {
	work_func_t exec;
	void* exec_arg;
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
    int                     t_bind;
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

#define gettid() ((pid_t)syscall(SYS_gettid))

extern void bonsai_self_thread_init();
extern void bonsai_self_thread_exit();

extern int bonsai_pflushd_thread_init();
extern int bonsai_pflushd_thread_exit();

extern int bonsai_user_thread_init(pthread_t tid);
extern void bonsai_user_thread_exit();

extern void wakeup_master();
extern void park_master();
extern void park_workers();
extern void wakeup_workers();

extern void wakeup_smo();

extern int bonsai_smo_thread_init();
extern int bonsai_smo_thread_exit();

extern int get_tid();

#ifdef __cplusplus
}
#endif

#endif
