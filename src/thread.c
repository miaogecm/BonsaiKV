/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <unistd.h>

#include "thread.h"
#include "cpu.h"
#include "atomic.h"
#include "common.h"
#include "bonsai.h"
#include "oplog.h"
#include "hp.h"
#include "hash_set.h"
#include "mptable.h"
#include "epoch.h"

__thread struct thread_info* __this = NULL;

#define CHKPT_INTERVAL		500

static pthread_mutex_t work_mutex;
static pthread_cond_t work_cond;

static atomic_t STATUS;
static atomic_t tids = ATOMIC_INIT(-1);

extern struct bonsai_info* bonsai;

inline int get_tid() {
	return __this->t_id;
}

static void init_workqueue(struct thread_info* thread, struct workqueue_struct* wq) {
	wq->thread = thread;
	INIT_LIST_HEAD(&wq->head);
}

void wakeup_workers() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, WORKER_RUNNING);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

static void try_wakeup_master() {
	if (atomic_sub_return(1, &STATUS) == WORKER_SLEEP) {
		pthread_mutex_lock(&work_mutex);
		pthread_cond_broadcast(&work_cond);
		pthread_mutex_unlock(&work_mutex);
	}
}

void wakeup_master() {
	pthread_mutex_lock(&work_mutex);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

void park_workers() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, WORKER_SLEEP);
	pthread_mutex_unlock(&work_mutex);
}

void park_master() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_SLEEP) 
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void worker_sleep() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_RUNNING)
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void thread_work(struct workqueue_struct* wq) {
	struct work_struct* work, *tmp;

	bonsai_debug("worker thread[%d] start to work\n", __this->t_id);

	if (likely(list_empty(&wq->head)))
		return;

	list_for_each_entry_safe(work, tmp, &wq->head, list) {
		work->func(work->arg);
		workqueue_del(work);
	}

	try_wakeup_master();
}

static void pflush_worker(struct thread_info* this) {
	__this = this;

	bind_to_cpu(__this->t_cpu + 3);

	bonsai_debug("pflush thread[%d] start on cpu[%d]\n", __this->t_id, get_cpu());

	thread_block_alarm();
	
	while (1) {
		worker_sleep();
		
		thread_work(&__this->t_wq);
	}
}

static void pflush_master(struct thread_info* this) {
	__this = this;

	bind_to_cpu(__this->t_cpu + 3);
	
	bonsai_debug("pflush thread[%d] start on cpu[%d]\n", __this->t_id, get_cpu());

	thread_block_alarm();

	for (;;) {
		
		usleep(CHKPT_INTERVAL);
		
		oplog_flush(bonsai);
	}
}

void bonsai_self_thread_init() {
	
	bonsai->self = malloc(sizeof(struct thread_info));
	bonsai->self->t_id = atomic_add_return(1, &tids);

	__this = bonsai->self;
}

void bonsai_self_thread_exit() {
	struct thread_info* thread = bonsai->self;
	
	free(thread);
}

void bonsai_user_thread_init() {
	struct thread_info* thread;

	thread = malloc(sizeof(struct thread_info));
	thread->t_id = atomic_add_return(1, &tids);
	thread->t_epoch = bonsai->desc->epoch;

	thread_set_alarm();

	__this = thread;
}

void bonsai_user_thread_exit() {
	struct thread_info* thread = __this;

	free(thread);
}

int bonsai_pflushd_thread_init() {
	struct thread_info* thread;
	int i;

	pthread_mutex_init(&work_mutex, NULL);
	pthread_cond_init(&work_cond, NULL);

	for (i = 0; i < NUM_PFLUSH_THREAD; i++) {
		thread = malloc(sizeof(struct thread_info));
		thread->t_id = i;
		thread->t_cpu = i;
		init_workqueue(thread, &thread->t_wq);
		thread->t_data = NULL;

		if (pthread_create(&bonsai->tids[i], NULL, 
			(i == 0) ? (void*)pflush_master : (void*)pflush_worker, (void*)thread) != 0) {
        		perror("bonsai create thread failed\n");
			return -ETHREAD;
    	}

		bonsai->pflushd[i] = thread;
	}
	
	return 0;
}

int bonsai_pflushd_thread_exit() {
	int i;

	for (i = 0; i < NUM_PFLUSH_THREAD; i++) {
		free(bonsai->pflushd[i]);
	}
	
	return 0;
}
