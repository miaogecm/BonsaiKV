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
#include <signal.h>
#include <sys/syscall.h>
#include <sys/types.h>

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

#define gettid() ((pid_t)syscall(SYS_gettid))

static pthread_mutex_t work_mutex;
static pthread_cond_t work_cond;

static atomic_t STATUS = ATOMIC_INIT(WORKER_SLEEP);
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
	while (atomic_read(&STATUS) != WORKER_SLEEP 
			&& atomic_read(&STATUS) != ALL_WAKEUP) 
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void worker_sleep() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_RUNNING 
			&& atomic_read(&STATUS) != ALL_WAKEUP)
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void wakeup_all() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, ALL_WAKEUP);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

void stop_signal_handler(int signo) {
	bonsai_print("thread[%d] stop\n", __this->t_id);
	
	__this->t_state = S_INTERRUPTED;
	sleep(60);
}

static int thread_register_stop_signal() {
	struct sigaction sa;
	int err = 0;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = stop_signal_handler;
	if (sigaction(SIGUSR1, &sa, NULL) == -1) {
		err = -ESIGNO;
	}

	return err;
}

void stop_the_world() {
	int i, states[NUM_THREAD] = {0};
	struct thread_info* thread;
	int num_interrupted = 0;
	
	states[__this->t_id] = S_INTERRUPTED;

	while (1) {
		list_for_each_entry(thread, &bonsai->thread_list, list) {
			if (unlikely(thread == __this))
				continue;
			
			if (kill(thread->t_pid, SIGUSR1)) {
				bonsai_print("thread[%d] stop the world fails %s\n", thread->t_pid, strerror(errno));
				exit(0);
			}

			usleep(50);

			if (states[thread->t_id] != S_INTERRUPTED && thread->t_state == S_INTERRUPTED) {
				states[thread->t_id] = S_INTERRUPTED;
				num_interrupted ++;
			}

			if (num_interrupted == NUM_THREAD - 1) {
				bonsai_print("world has been stopped\n");
				return;
			}
		}
	}
}

static void thread_work(struct workqueue_struct* wq) {
	struct work_struct* work, *tmp;
	int ret = 0;

	bonsai_debug("worker thread[%d] start to work\n", __this->t_id);

	if (likely(list_empty(&wq->head)))
		return;

	list_for_each_entry_safe(work, tmp, &wq->head, list) {
		ret = work->exec(work->exec_arg);
		workqueue_del(work);

		if (unlikely(ret)) 
			return;
	}

	try_wakeup_master();
}

static void pflush_thread_exit(struct thread_info* thread) {
	struct log_layer *layer = LOG(bonsai);

	list_del(&thread->list);
	thread->t_state = S_EXIT;

	atomic_inc(&layer->exit);
	
	bonsai_print("pflush thread[%d] exit\n", thread->t_id);
}

static void pflush_worker(struct thread_info* this) {
	struct log_layer *layer = LOG(bonsai);

	__this = this;
	__this->t_pid = gettid();
	__this->t_state = S_RUNNING;

	bind_to_cpu(__this->t_cpu);

	bonsai_print("pflush thread[%d] pid[%d] start on cpu[%d]\n", 
			__this->t_id, __this->t_pid, get_cpu());

	thread_block_alarm_signal();
	thread_register_stop_signal();
	
	while (!atomic_read(&layer->exit)) {
		__this->t_state = S_SLEEPING;
		worker_sleep();
		
		__this->t_state = S_RUNNING;
		thread_work(&__this->t_wq);
	}

	pflush_thread_exit(this);
}

static void master_wait_workers(struct thread_info* master) {
	struct thread_info* thread;
	int i, num_worker = 0;

	while (1) {
		for (i = 0; i < NUM_PFLUSH_THREAD; i ++) {
			thread = bonsai->pflushd[i];
			if (thread != master) {
				if (thread->t_state == S_UNINIT)
					usleep(10);
				else
					num_worker ++;

				if (num_worker == NUM_PFLUSH_THREAD - 1)
					return;
			}
		}
	}
}

static void pflush_master(struct thread_info* this) {
	struct log_layer *layer = LOG(bonsai);
	
	__this = this;
	__this->t_pid = gettid();
	__this->t_state = S_RUNNING;

	bind_to_cpu(__this->t_cpu);
	
	bonsai_print("pflush thread[%d] pid[%d] start on cpu[%d]\n", __this->t_id, __this->t_pid, get_cpu());

	thread_block_alarm_signal();
	thread_register_stop_signal();

	while (!atomic_read(&layer->exit)) {
		__this->t_state = S_SLEEPING;
		//usleep(CHKPT_TIME_INTERVAL);
		park_master();

		__this->t_state = S_RUNNING;
		oplog_flush(bonsai);
	}

	pflush_thread_exit(this);
}

void bonsai_self_thread_init() {
	
	bonsai->self = malloc(sizeof(struct thread_info));
	bonsai->self->t_id = atomic_add_return(1, &tids);
	bonsai->self->t_pid = gettid();
	bonsai->self->t_state = S_RUNNING;

	thread_register_stop_signal();

	__this = bonsai->self;

	list_add(&__this->list, &bonsai->thread_list);

	bonsai_print("self thread[%d] pid[%d] start on cpu[%d]\n", __this->t_id, __this->t_pid, get_cpu());
}

void bonsai_self_thread_exit() {
	struct thread_info* thread = bonsai->self;

	list_del(&thread->list);
	
	free(thread);
}

int bonsai_user_thread_init() {
	struct thread_info* thread;
	int ret;

	thread = malloc(sizeof(struct thread_info));
	thread->t_id = atomic_add_return(1, &tids);
	thread->t_pid = gettid();
	thread->t_state = S_RUNNING;
	thread->t_epoch = bonsai->desc->epoch;

	list_add(&thread->list, &bonsai->thread_list);

	thread_register_alarm_signal();
	thread_register_stop_signal();

	__this = thread;

	bonsai_print("user thread[%d] pid[%d] start on cpu[%d]\n", __this->t_id, __this->t_pid, get_cpu());

	return 0;
}

void bonsai_user_thread_exit() {
	struct thread_info* thread = __this;

	list_del(&thread->list);

	free(thread);
}

int bonsai_pflushd_thread_init() {
	struct thread_info* thread;
	int i;

	pthread_mutex_init(&work_mutex, NULL);
	pthread_cond_init(&work_cond, NULL);

	for (i = 0; i < NUM_PFLUSH_THREAD; i++) {
		thread = malloc(sizeof(struct thread_info));
		thread->t_id = atomic_add_return(1, &tids);
		thread->t_cpu = i + 4;
		thread->t_state = S_UNINIT;
		init_workqueue(thread, &thread->t_wq);
		thread->t_data = NULL;

		list_add(&thread->list, &bonsai->thread_list);

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
	struct log_layer* layer = LOG(bonsai);
	volatile int num_exit = 0;
	int i;

	atomic_set(&layer->exit, 1);

	while (atomic_read(&layer->exit) != (NUM_PFLUSH_THREAD + 1)) {
		wakeup_all();
		usleep(10);
	}

	for (i = 0; i < NUM_PFLUSH_THREAD; i++) {
		pthread_join(bonsai->tids[i], NULL);
		free(bonsai->pflushd[i]);
	}

	bonsai_print("pflush thread exit\n");
	
	return 0;
}
